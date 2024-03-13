import sys
import time
import threading
from client.client import RaftClient
from nodes.raft_node import RaftNode
import utils
from config import Config

def start_cluster():
    # Start the cluster with specified number of nodes
    nodes = []
    for i in range(Config.NUM_NODES):
        node = RaftNode(ip=Config.NODE_IPS[i], port=Config.NODE_PORTS[i])
        node.start()
        nodes.append(node)
    return nodes

def wait_for_leader_election(nodes):
    # Wait for the leader to be elected and a NO-OP entry to be appended in all the logs
    leader = None
    while leader is None:
        for node in nodes:
            if node.state == 'LEADER':
                leader = node
                break
        time.sleep(1)  # Adjust sleep time as needed
    print("Leader elected:", leader.ip, leader.port)

def perform_set_and_get_requests(client):
    # Perform 3 set requests
    for i in range(3):
        key = f"key_{i}"
        value = f"value_{i}"
        RaftClient.set(key, value)
        print(f"Set {key}={value}")
    
    # Perform 3 get requests
    for i in range(3):
        key = f"key_{i}"
        value = RaftClient.get(key)
        print(f"Get {key}={value}")

def terminate_nodes(nodes, count):
    # Terminate the specified number of follower nodes
    terminated_nodes = []
    for i in range(count):
        node = nodes.pop()
        node.terminate()
        terminated_nodes.append(node)
    return terminated_nodes

def restart_nodes(nodes):
    # Restart terminated follower nodes
    for node in nodes:
        node.restart()

def simulate_leader_failure(nodes):
    # Terminate the current leader process
    leader = None
    for node in nodes:
        if node.state == 'LEADER':
            leader = node
            break
    if leader:
        leader.terminate()

def test_leader_failure(nodes):
    leader = None
    for node in nodes:
        if node.state == 'LEADER':
            leader = node
            break
    simulate_leader_failure(nodes)
    wait_for_leader_election(nodes)
    client = RaftClient("127.0.0.1", 5000)  # Use any node IP and port for client
    perform_set_and_get_requests(client)
    restart_nodes([leader])

def simulate_lease_timeout(nodes):
    # Terminate 3 (majority) follower nodes
    terminate_nodes(nodes, 3)
    # Wait for the leader's lease to timeout and step down
    time.sleep(5)  # Adjust sleep time based on lease timeout
    # Restart terminated nodes
    restart_nodes(nodes)

def test_lease_timeout(nodes):
    simulate_lease_timeout(nodes)
    client = RaftClient("127.0.0.1", 5000)  # Use any node IP and port for client
    perform_set_and_get_requests(client)

def simulate_leader_absence(nodes):
    # Terminate all nodes except two follower nodes
    terminate_nodes(nodes, 3)
    # Wait for leader absence
    time.sleep(5)  # Adjust sleep time as needed

def test_leader_absence(nodes):
    simulate_leader_absence(nodes)
    client = RaftClient("127.0.0.1", 5000)  # Use any node IP and port for client
    perform_set_and_get_requests(client)

def main():
    nodes = start_cluster()
    
    # Test leader election and basic functionality
    wait_for_leader_election(nodes)
    client = RaftClient("127.0.0.1", 5000)  # Use any node IP and port for client
    perform_set_and_get_requests(client)
    
    # Test fault tolerance and follower catch-up
    terminated_nodes = terminate_nodes(nodes, 2)
    perform_set_and_get_requests(client)
    restart_nodes(terminated_nodes)
    
    # Test leader failure, leader election, and log replication
    test_leader_failure(nodes)
    
    # Test leader lease timeout
    test_lease_timeout(nodes)
    
    # Test failure in absence of leader in the system
    test_leader_absence(nodes)

if __name__ == "__main__":
    main()
