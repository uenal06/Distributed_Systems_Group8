import sys
import os
import uuid
import json
from time import sleep

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from middleware import Middleware


class StateMachine:
    peer_uuid = str(uuid.uuid4())
    current_state = "initializing"

    def __init__(self):
        self.peers_ids = []
        self.round_options = []
        self.votes_by_peer = {}
        self.has_voted = False
        self.middleware = Middleware(self.peer_uuid, self)
        # Listen for peers dropping during a vote
        self.middleware.subscribe_tcp_unicast_listener(self.handle_lost_peer)

        # Define state actions
        self.state_actions = {
            "initializing": {"run": self.run_initializing},
            "lobby": {"entry": self.enter_lobby, "run": self.run_lobby},
            "await_peers": {"entry": self.enter_await_peers, "run": self.run_await_peers},
            "start_new_round": {"run": self.run_start_new_round},
            "collect_votes": {
                "entry": self.enter_collect_votes,
                "run": self.run_collect_votes,
                "exit": self.exit_collect_votes
            },
            "await_round_start": {
                "entry": self.enter_await_round_start,
                "run": self.run_await_round_start,
                "exit": self.exit_await_round_start
            },
            "vote_phase": {
                "entry": self.enter_vote_phase,
                "run": self.run_vote_phase,
                "exit": self.exit_vote_phase
            }
        }

    def run_initializing(self):
        print(f"UUID: {self.middleware._my_uuid}")
        port = input("Select Voting Room Port (Leave Empty for default value of 61424): ")
        self.add_peer(self.middleware._my_uuid)
        self.switch_to_state("lobby")

    def enter_lobby(self):
        self.middleware._leader_uuid = self.middleware._my_uuid

        self.middleware.subscribe_tcp_unicast_listener(self.handle_start_voting)
        self.middleware.subscribe_broadcast_listener(self.send_peer_list)
        self.middleware.subscribe_tcp_unicast_listener(self.listen_for_peers_list)

        print("\nSent Broadcast!")
        self.middleware.broadcast_to_all("enterLobby", self.middleware._my_uuid)

    def run_lobby(self):
        sleep(0.5)
        next_state = (
            "await_peers"
            if self.middleware._leader_uuid == self.middleware._my_uuid
            else "await_round_start"
        )
        self.switch_to_state(next_state)

    def enter_await_peers(self):
        if len(self.peers_ids) < 2:
            print("Waiting for more peers - 2 peers needed at minimum!")

    def run_await_peers(self):
        if len(self.peers_ids) >= 2:
            self.display_lobby()
            self.switch_to_state("start_new_round")

    def run_start_new_round(self):
        if len(self.peers_ids) < 2:
            print(f"\nNot enough peers ({len(self.peers_ids)}) to start a round. Waiting for more…")
            self.switch_to_state("await_peers")
            return

        print("\nStart a new food vote. Enter the food options.")
        food_options = []
        while True:
            option = input("Enter food option (leave blank to finish): ")
            if not option:
                break
            food_options.append(option)

        self.round_options   = food_options
        self.votes_by_peer   = {}
        self.has_voted       = False

        # Broadcast to everyone
        self.middleware.multicast_reliable("startVoting", json.dumps(food_options))
        print(f"Multicasted food voting options: {food_options}")

        self.switch_to_state("collect_votes")

    def enter_collect_votes(self):
        self.middleware.subscribe_tcp_unicast_listener(self.handle_vote_message)

    def run_collect_votes(self):
        pass  # Leader does not vote

    def exit_collect_votes(self):
        self.middleware.unsubscribe_tcp_unicast_listener(self.handle_vote_message)

    def enter_await_round_start(self):
        self.middleware.subscribe_tcp_unicast_listener(self.handle_start_voting)
        self.middleware.subscribe_tcp_unicast_listener(self.handle_vote_message)
        self.middleware.subscribe_tcp_unicast_listener(self.handle_result_message)

    def run_await_round_start(self):
        sleep(0.1)
        if self.round_options:
            self.switch_to_state("vote_phase")

    def exit_await_round_start(self):
        self.middleware.unsubscribe_tcp_unicast_listener(self.handle_start_voting)
        self.middleware.unsubscribe_tcp_unicast_listener(self.handle_vote_message)
        self.middleware.unsubscribe_tcp_unicast_listener(self.handle_result_message)

    def enter_vote_phase(self):
        self.middleware.subscribe_tcp_unicast_listener(self.handle_start_voting)
        self.middleware.subscribe_tcp_unicast_listener(self.handle_vote_message)
        self.middleware.subscribe_tcp_unicast_listener(self.handle_result_message)

    def run_vote_phase(self):
        if not self.round_options or self.has_voted:
            return

        print("\nVote for one of the following food options:")
        for i, option in enumerate(self.round_options, start=1):
            print(f"{i}) {option}")

        try:
            choice = int(input("Enter number of your choice: ")) - 1
            vote = self.round_options[choice]
            self.middleware.multicast_reliable("vote", vote)
            self.handle_vote_message(self.middleware._my_uuid, 'vote', vote)
            self.has_voted = True
        except Exception:
            print("Invalid input. Try again.")

    def exit_vote_phase(self):
        self.middleware.unsubscribe_tcp_unicast_listener(self.handle_start_voting)
        self.middleware.unsubscribe_tcp_unicast_listener(self.handle_vote_message)
        self.middleware.unsubscribe_tcp_unicast_listener(self.handle_result_message)
        self.has_voted     = False
        self.round_options = []

    def calc_and_declare_winner(self):
        results = {}
        for vote in self.votes_by_peer.values():
            results[vote] = results.get(vote, 0) + 1

        print("\nVoting completed. Results:")
        for food, count in results.items():
            print(f"{food}: {count} votes")
        winner = max(results.items(), key=lambda x: x[1])
        print(f"\nThe winner is: {winner[0]} with {winner[1]} votes!\n")

        payload = json.dumps({
            "results": results,
            "winner":  winner[0],
            "votes":   winner[1]
        })
        self.middleware.multicast_reliable("votingResult", payload)

        # Reset for next round
        self.round_options = []
        self.votes_by_peer = {}
        self.has_voted     = False
        self.switch_to_state("start_new_round")

    def listen_for_peers_list(self, messenger_uuid, command, peers_list):
        if command == 'peerList':
            self.middleware._leader_uuid = messenger_uuid
            self.update_peers(peers_list)

    def send_peer_list(self, messenger_uuid, command, payload):
        if command == 'enterLobby':
            self.add_peer(messenger_uuid)

            if self.middleware._my_uuid == self.middleware._leader_uuid:
                self.middleware.send_ip_addresses_to(messenger_uuid)
                self.middleware.send_tcp_message_to(
                    messenger_uuid,
                    'peerList',
                    self.peers_list_string()
                )

                if self.current_state in ('collect_votes', 'vote_phase'):
                    self.middleware.send_tcp_message_to(
                        messenger_uuid,
                        'startVoting',
                        json.dumps(self.round_options)
                    )

    def handle_start_voting(self, messenger_uuid, message_command, message_data):
        if message_command == "startVoting":
            print(f"Received voting options from {messenger_uuid}: {message_data}")
            self.round_options  = json.loads(message_data)
            self.votes_by_peer  = {}
            self.has_voted      = False

    def handle_vote_message(self, messenger_uuid, message_command, message_data):
        if message_command == "vote":
            self.votes_by_peer[messenger_uuid] = message_data
            if (self.middleware._leader_uuid == self.middleware._my_uuid and
                len(self.votes_by_peer) == len(self.peers_ids) - 1):
                self.calc_and_declare_winner()

    def handle_result_message(self, messenger_uuid, message_command, message_data):
        if message_command == "votingResult":
            result_data = json.loads(message_data)
            print(f"\nVoting results from {messenger_uuid}:")
            for food, count in result_data["results"].items():
                print(f"{food}: {count} votes")
            print(f"Winner: {result_data['winner']} with {result_data['votes']} votes!")

    def handle_lost_peer(self, messenger_uuid, command, data):
        if command == 'lostpeer':
            print(f"Peer {data} left the voting session")
            self.remove_peer(data)
            if (self.current_state == 'collect_votes' and
                self.middleware._leader_uuid == self.middleware._my_uuid and
                len(self.votes_by_peer) == len(self.peers_ids) - 1):
                self.calc_and_declare_winner()

    def switch_to_state(self, target_state):
        if (self.current_state in self.state_actions and
            "exit" in self.state_actions[self.current_state]):
            self.state_actions[self.current_state]["exit"]()

        self.current_state = target_state

        if (target_state in self.state_actions and
            "entry" in self.state_actions[target_state]):
            self.state_actions[target_state]["entry"]()

    def run_loop(self):
        while True:
            action = self.state_actions.get(self.current_state, {}).get("run")
            if action:
                action()

    def add_peer(self, uuid):
        if uuid not in self.peers_ids:
            self.peers_ids.append(uuid)

    def remove_peer(self, uuid):
        if uuid in self.peers_ids:
            self.peers_ids.remove(uuid)

    def peers_list_string(self):
        return ','.join(self.peers_ids)

    def update_peers(self, peers_list):
        self.peers_ids = peers_list.split(',') if peers_list else []

    def display_lobby(self):
        print(f"\nLobby: {len(self.peers_ids)} peers")
        for i, peer_id in enumerate(self.peers_ids, start=1):
            print(f"{i}. {peer_id}")


if __name__ == "__main__":
    state_machine = StateMachine()
    state_machine.run_loop()

    
