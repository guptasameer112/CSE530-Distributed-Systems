'''
This file contains configuration parameters for the Raft cluster, such as 
election timeouts, 
leader lease durations, and 
network addresses of other nodes in the cluster.
'''

ELECTION_TIMEOUT_MIN = 1000
ELECTION_TIMEOUT_MAX = 2000
HEARTBEAT_INTERVAL = 500
LEADER_LEASE_DURATION = 10000

NODE_ADDRESSES = {
    'node1': 'localhost:5001',
    'node2': 'localhost:5002',
    'node3': 'localhost:5003'
}
