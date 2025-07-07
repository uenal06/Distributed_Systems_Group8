import threading
import socket
import ipaddress
from time import sleep
from dataclasses import dataclass

# Network configuration constants
BROADCAST_PORT = 61424
BUFFER_SIZE = 1024
SUBNET_MASK = "255.255.255.0"
HEARTBEAT_TIMEOUT = 3
HEARTBEAT_INTERVAL = 1
TCP_LISTEN_BACKLOG = 5
SOCKET_TIMEOUT = 60


def get_local_ip():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.connect(("8.8.8.8", 80))
        return sock.getsockname()[0]
    finally:
        sock.close()


#Configuration for network settings.
@dataclass
class NetworkConfig:
    local_ip: str = get_local_ip()
    broadcast_ip: str = ipaddress.IPv4Network(
        get_local_ip() + '/' + SUBNET_MASK, False
    ).broadcast_address.exploded


#Base class for message handling with observer pattern.
class MessageHandler:
    
    def __init__(self):
        self._listeners = []
    
    def subscribe(self, observer_func):
        self._listeners.append(observer_func)
    
    def unsubscribe(self, observer_func):
        self._listeners = [f for f in self._listeners if f != observer_func]
    
    #Notify all subscribed listeners.
    def _notify_listeners(self, *args, **kwargs):
        for listener in self._listeners:
            try:
                listener(*args, **kwargs)
            except Exception as e:
                print(f"Error in message listener: {e}")


class BroadcastHandler(MessageHandler):

    def __init__(self, local_ip, broadcast_ip, my_uuid, server_port, middleware):
        super().__init__()
        self._local_ip = local_ip
        self._broadcast_ip = broadcast_ip
        self._my_uuid = my_uuid
        self._server_port = server_port
        self._middleware = middleware
        
        # Initialize sockets
        self._broadcast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._listen_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._listen_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self._listen_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._listen_socket.bind((local_ip, BROADCAST_PORT))
        
        # Start listening thread
        self._listen_thread = threading.Thread(target=self._listen_udp_broadcast, daemon=True)
        self._listen_thread.start()
    
    #Broadcast a message to all peers on the network.
    def broadcast(self, message):
        packet = f"{self._my_uuid}_{self._local_ip}_{self._server_port}_{message}"
        self._broadcast_socket.sendto(packet.encode('utf-8'), (self._broadcast_ip, BROADCAST_PORT))
    
    def _listen_udp_broadcast(self):
        while True:
            try:
                data, _ = self._listen_socket.recvfrom(BUFFER_SIZE)
                decoded = data.decode('utf-8')
                messenger_uuid, messenger_ip, messenger_port, message = decoded.split('_', 3)
                
                # Add the sender to known peers
                if self._middleware:
                    self._middleware.add_ip_address(messenger_uuid, (messenger_ip, int(messenger_port)))
                
                # Notify listeners about the message
                if messenger_uuid != self._my_uuid:
                    command, payload = message.split(':', 1)
                    self._notify_listeners(messenger_uuid, command, payload)
                    
            except Exception as e:
                print(f"Error in broadcast listener: {e}")


class UdpUnicastHandler(MessageHandler):

    def __init__(self, my_uuid, local_ip):
        super().__init__()
        self._my_uuid = my_uuid
        self._local_ip = local_ip
        
        # Initialize socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._server_socket.bind(('', 0))
        self._server_port = self._server_socket.getsockname()[1]
        
        # Start listening thread
        self._listen_thread = threading.Thread(target=self._listen_unicast, daemon=True)
        self._listen_thread.start()

    @property
    def server_port(self):
        return self._server_port
    
    #Send a UDP message to a specific address.
    def send_message(self, addr, message):
        packet = f"{self._my_uuid}_{self._local_ip}_{self._server_port}_{message}"
        self._server_socket.sendto(packet.encode('utf-8'), addr)

    #Listen for UDP unicast messages.
    def _listen_unicast(self):
        while True:
            try:
                data, _ = self._server_socket.recvfrom(BUFFER_SIZE)
                msg = data.decode('utf-8').split('_', 3)
                messenger_uuid, messenger_ip, messenger_port, message = msg
                command, payload = message.split(':', 1)
                self._notify_listeners(messenger_uuid, command, payload)
            except Exception as e:
                pass
                #print(f"Error in unicast listener: {e}")


class TcpUnicastHandler(MessageHandler):
    
    def __init__(self, my_uuid, local_ip, server_port):
        super().__init__()
        self._my_uuid = my_uuid
        self._local_ip = local_ip
        self._server_port = server_port
        
        # Initialize socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', server_port))
        
        # Start listening thread
        self._listen_thread = threading.Thread(target=self._listen_tcp_unicast, daemon=True)
        self._listen_thread.start()

    #Send a TCP message to a specific address.
    def send_message(self, addr, message):
        threading.Thread(
            target=self._send_message_thread, 
            args=(addr, message), 
            daemon=True
        ).start()
    
    def _send_message_thread(self, addr, message):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(('', 0))
        try:
            sock.connect(addr)
            packet = f"{self._my_uuid}_{self._local_ip}_{self._server_port}_{message}"
            sock.send(packet.encode('utf-8'))
        except ConnectionRefusedError:
            print(f"Connection refused to {addr}")
        except Exception as e:
            print(f"Error sending TCP message to {addr}: {e}")
        finally:
            sock.close()

    def _listen_tcp_unicast(self):
        self._server_socket.listen(TCP_LISTEN_BACKLOG)
        while True:
            try:
                client_socket, _ = self._server_socket.accept()
                client_socket.settimeout(SOCKET_TIMEOUT)
                threading.Thread(
                    target=self._listen_to_client, 
                    args=(client_socket,), 
                    daemon=True
                ).start()
            except Exception as e:
                print(f"Error accepting TCP connection: {e}")
    
    #Handle communication with a TCP client.
    def _listen_to_client(self, client_socket):
        try:
            data = client_socket.recv(BUFFER_SIZE).decode('utf-8')
            if data:
                messenger_uuid, messenger_ip, messenger_port, message = data.split('_', 3)
                command, payload = message.split(':', 1)
                self._notify_listeners(messenger_uuid, command, payload)
        except Exception as e:
            print(f"Error handling TCP client: {e}")
        finally:
            client_socket.close()


class Middleware:
    """
    Main middleware class coordinating communication and system state.
    
    This class manages peer-to-peer communication, heartbeat monitoring,
    leader election, and reliable message delivery in a distributed system.
    """
    
    def __init__(self, uuid, state_machine):

        # Network configuration
        self._network_config = NetworkConfig()
        self._my_uuid = uuid
        self._state_machine = state_machine
        
        # Peer management
        self._ip_addresses = {}
        self._neighbor_uuid = None
        self._neighbor_alive = False
        self._leader_uuid = ''
        
        # Initialize communication handlers
        self._udp_handler = UdpUnicastHandler(self._my_uuid, self._network_config.local_ip)
        self._tcp_handler = TcpUnicastHandler(
            self._my_uuid, 
            self._network_config.local_ip, 
            self._udp_handler.server_port
        )
        self._broadcast_handler = BroadcastHandler(
            self._network_config.local_ip,
            self._network_config.broadcast_ip,
            self._my_uuid,
            self._udp_handler.server_port,
            self
        )
        
        self._setup_message_handlers()
        
        # Start heartbeat monitoring
        self._heartbeat_thread = threading.Thread(target=self._send_heartbeats, daemon=True)
        self._heartbeat_thread.start()
    
    def _setup_message_handlers(self):
        self._tcp_handler.subscribe(self._update_addresses)
        self._tcp_handler.subscribe(self._check_for_voting_announcement)
        self._udp_handler.subscribe(self._listen_heartbeats)
        self._tcp_handler.subscribe(self._listen_lost_peer)
    
    #Find the neighbor UUID in the ring topology.
    def find_neighbor(self, own_uuid, ip_addresses):
        ordered = sorted(ip_addresses.keys())
        if own_uuid in ordered:
            own_index = ordered.index(own_uuid)
            neighbor_index = (own_index - 1) % len(ordered)
            return ordered[neighbor_index] if ordered[neighbor_index] != own_uuid else None
        return None
    
    #Send periodic heartbeat messages to monitor neighbor health.
    def _send_heartbeats(self):
        consecutive_failures = 0
        
        while True:
            self._neighbor_alive = False
            
            if not self._neighbor_uuid:
                self._neighbor_uuid = self.find_neighbor(self._my_uuid, self._ip_addresses)
                sleep(HEARTBEAT_INTERVAL)
                continue
            
            # Send heartbeat ping
            self.send_message_to(self._neighbor_uuid, 'hbping', self._my_uuid)
            sleep(HEARTBEAT_INTERVAL)
            
            # Check responses
            if not self._neighbor_alive:
                consecutive_failures += 1
                if consecutive_failures >= HEARTBEAT_TIMEOUT:
                    self._handle_neighbor_loss()
                    consecutive_failures = 0
            else:
                consecutive_failures = 0
    
    def _handle_neighbor_loss(self):
        lost_neighbor = self._neighbor_uuid
        self._ip_addresses.pop(lost_neighbor, None)
        self._state_machine.remove_peer(lost_neighbor)
        self.multicast_reliable('lostpeer', lost_neighbor)
        
        if lost_neighbor == self._leader_uuid:
            print(f"Leader {self._leader_uuid} lost, initiating leader election")
            self.initiate_voting()
        
        self._neighbor_uuid = None
    
    def _listen_heartbeats(self, messenger_uuid, command, data):
        if command == 'hbping':
            self.send_message_to(messenger_uuid, 'hbresponse', self._my_uuid)
        elif command == 'hbresponse':
            if messenger_uuid == self._neighbor_uuid:
                self._neighbor_alive = True
    
    def _listen_lost_peer(self, messenger_uuid, command, data):
        if command == 'lostpeer':
            self._ip_addresses.pop(data, None)
            self._neighbor_uuid = None
            self._state_machine.remove_peer(data)
    
    def add_ip_address(self, uuid, addr):
        self._ip_addresses[uuid] = addr
        self._neighbor_uuid = None
    
    def broadcast_to_all(self, command, data=''):
        self._broadcast_handler.broadcast(f"{command}:{data}")
    
    #Send a UDP message to a specific peer.
    def send_message_to(self, uuid, command, data=''):
        if uuid in self._ip_addresses:
            ip_address = self._ip_addresses[uuid]
            self._udp_handler.send_message(ip_address, f"{command}:{data}")
    
    #Send a TCP message to a specific peer.
    def send_tcp_message_to(self, uuid, command, data=''):
        if uuid in self._ip_addresses:
            addr = self._ip_addresses[uuid]
            self._tcp_handler.send_message(addr, f"{command}:{data}")
    
    #Send a reliable message to all peers except self.
    def multicast_reliable(self, command, data=''):
        message = f"{command}:{data}"
        for peer_uuid, addr in self._ip_addresses.items():
            if peer_uuid != self._my_uuid:
                self._tcp_handler.send_message(addr, message)
    
    #Send the list of known IP addresses to a specific peer.
    def send_ip_addresses_to(self, uuid):
        command = 'updateIpAdresses'
        address_string = f"{self._leader_uuid}$"
        for uid, (addr, port) in self._ip_addresses.items():
            address_string += f"{uid},{addr},{port}#"
        self.send_tcp_message_to(uuid, command, address_string)
    
    def subscribe_broadcast_listener(self, observer_func):
        self._broadcast_handler.subscribe(observer_func)
    
    def subscribe_unicast_listener(self, observer_func):
        self._udp_handler.subscribe(observer_func)
    
    def subscribe_tcp_unicast_listener(self, observer_func):
        self._tcp_handler.subscribe(observer_func)
    
    def unsubscribe_tcp_unicast_listener(self, observer_func):
        self._tcp_handler.unsubscribe(observer_func)
    
    #Update the list of known peer addresses.
    def _update_addresses(self, messenger_uuid, command, data):
        if command == 'updateIpAdresses':
            parts = data.split('$')
            self._leader_uuid = parts[0]
            addr_list = parts[1].rstrip('#').split('#')
            for entry in addr_list:
                uid, ip, port = entry.split(',')
                self.add_ip_address(uid, (ip, int(port)))
    
    #Handle voting announcements for leader election.
    def _check_for_voting_announcement(self, messenger_uuid, command, data):
        if command == 'voting':
            print(f"Received voting announcement with UUID {data}")
            if data == self._my_uuid:
                print(f"You are the new leader with UUID: {self._my_uuid}")
                self._leader_uuid = self._my_uuid
                # Reset state machine
                self._state_machine.round_options = []
                self._state_machine.votes_by_peer = {}
                self._state_machine.has_voted = False
                self.subscribe_tcp_unicast_listener(self._state_machine.handle_vote_message)
                self._state_machine.switch_to_state("start_new_round")
                self.multicast_reliable('leaderElected', self._my_uuid)
            else:
                # Forward to next node in ring
                next_uuid = self._find_lower_neighbor() 
                print(f"Forwarding voting token ({data}) to {next_uuid}")
                self.send_tcp_message_to(next_uuid, 'voting', data)
        elif command == 'leaderElected':
            print(f"New leader elected: {data}")
            self._leader_uuid = data
            self._state_machine.switch_to_state("await_round_start")
    
    #Initiate a leader election process.
    def initiate_voting(self):
        lower = self._find_lower_neighbor()
        print(f"Initiating leader election: sending 'voting' to {lower}")
        self.send_tcp_message_to(lower, 'voting', self._my_uuid)
    
    #Find the neighbor with a lower UUID in the ring.
    def _find_lower_neighbor(self):
        ordered = sorted(self._ip_addresses.keys())
        idx = ordered.index(self._my_uuid)
        return ordered[idx - 1]
