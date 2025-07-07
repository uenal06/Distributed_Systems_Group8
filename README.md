# Distributed_Systems_Group8
The code implements a peer-to-peer food-voting system: peers discover each other via UDP broadcast, form a ring and elect a leader, the leader multicasts voting options and collects votes over TCP, announces the results, and uses UDP heartbeat pings plus multithreading to detect and recover from crashed peers.
