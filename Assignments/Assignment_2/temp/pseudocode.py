import time
import random
from collections import defaultdict

class RaftNode:
    def __init__(self, node_id, nodes):
        self.id = node_id
        self.nodes = nodes
        self.reset_state()

    def reset_state(self):
        self.currentTerm = 0
        self.votedFor = None
        self.log = []
        self.commitLength = 0
        self.currentRole = 'follower'
        self.currentLeader = None
        self.votesReceived = set()
        self.sentLength = defaultdict(int)
        self.ackedLength = defaultdict(int)

    def calculate_election_timeout(self):
        return time.time() + random.uniform(0.15, 0.3)

    def start_election(self):
        self.currentTerm += 1
        self.currentRole = 'candidate'
        self.votedFor = self.id
        self.votesReceived = {self.id}
        lastTerm = 0
        if self.log:
            lastTerm = self.log[-1].term
        msg = ("VoteRequest", self.id, self.currentTerm, len(self.log), lastTerm)
        for node in self.nodes:
            self.send_message(node, msg)
        self.election_timeout = self.calculate_election_timeout()

    def receive_vote_request(self, candidate_id, term, log_length, log_term):
        if term > self.currentTerm:
            self.currentTerm = term
            self.currentRole = 'follower'
            self.votedFor = None
            self.election_timeout = self.calculate_election_timeout()

        lastTerm = 0
        if self.log:
            lastTerm = self.log[-1].term

        log_ok = (log_term > lastTerm) or \
                 (log_term == lastTerm and log_length >= len(self.log))

        if term == self.currentTerm and log_ok and (self.votedFor is None or self.votedFor == candidate_id):
            self.votedFor = candidate_id
            return ("VoteResponse", self.id, self.currentTerm, True)
        else:
            return ("VoteResponse", self.id, self.currentTerm, False)

    def collect_votes(self):
        if len(self.votesReceived) >= (len(self.nodes) + 1) // 2:
            self.currentRole = 'leader'
            self.currentLeader = self.id
            for follower in self.nodes:
                if follower != self.id:
                    self.sentLength[follower] = len(self.log)
                    self.ackedLength[follower] = 0
                    self.replicate_log(self.id, follower)

    def replicate_log(self, leader_id, follower_id):
        prefixLen = self.sentLength[follower_id]
        suffix = self.log[prefixLen:]
        prefixTerm = 0
        if prefixLen > 0:
            prefixTerm = self.log[prefixLen - 1].term
        msg = ("LogRequest", leader_id, self.currentTerm, prefixLen, prefixTerm, self.commitLength, suffix)
        self.send_message(follower_id, msg)

    def receive_log_request(self, leader_id, term, prefixLen, prefixTerm, leaderCommit, suffix):
        if term > self.currentTerm:
            self.currentTerm = term
            self.currentRole = 'follower'
            self.votedFor = None
            self.election_timeout = self.calculate_election_timeout()

        if term == self.currentTerm:
            self.currentRole = 'follower'
            self.currentLeader = leader_id

        log_ok = (len(self.log) >= prefixLen) and \
                 ((prefixLen == 0) or (self.log[prefixLen - 1].term == prefixTerm))

        if term == self.currentTerm and log_ok:
            self.append_entries(prefixLen, leaderCommit, suffix)
            ack = prefixLen + len(suffix)
            return ("LogResponse", self.id, self.currentTerm, ack, True)
        else:
            return ("LogResponse", self.id, self.currentTerm, 0, False)

    def append_entries(self, prefixLen, leaderCommit, suffix):
        if suffix and len(self.log) > prefixLen:
            index = min(len(self.log), prefixLen + len(suffix)) - 1
            if self.log[index].term != suffix[index - prefixLen].term:
                self.log = self.log[:prefixLen]

        if prefixLen + len(suffix) > len(self.log):
            for i in range(len(self.log) - prefixLen, len(suffix)):
                self.log.append(suffix[i])

        if leaderCommit > self.commitLength:
            for i in range(self.commitLength, leaderCommit):
                print(f"Deliver message: {self.log[i].msg}")
            self.commitLength = leaderCommit

    def receive_log_response(self, follower, term, ack, success):
        if term == self.currentTerm and self.currentRole == 'leader':
            if success and ack >= self.ackedLength[follower]:
                self.sentLength[follower] = ack
                self.ackedLength[follower] = ack
                self.commit_log_entries()
            elif self.sentLength[follower] > 0:
                self.sentLength[follower] -= 1
                self.replicate_log(self.id, follower)
        elif term > self.currentTerm:
            self.currentTerm = term
            self.currentRole = 'follower'
            self.votedFor = None
            self.election_timeout = self.calculate_election_timeout()

    def commit_log_entries(self):
        minAcks = (len(self.nodes) + 1) // 2
        ready = [length for length in range(1, len(self.log) + 1) if len([node for node in self.nodes if self.ackedLength[node] >= length]) >= minAcks]
        if ready and max(ready) > self.commitLength and self.log[max(ready) - 1].term == self.currentTerm:
            for i in range(self.commitLength, max(ready)):
                print(f"Deliver message: {self.log[i].msg}")
            self.commitLength = max(ready)

    def send_message(self, node, msg):
        # Implement message sending logic
        pass

    def receive_message(self, msg):
        # Implement message receiving logic
        pass

class LogEntry:
    def __init__(self, term, msg):
        self.term = term
        self.msg = msg

def main():
    nodes = [0, 1, 2]  # List of node IDs
    node_id = 0  # ID of the current node

    raft_node = RaftNode(node_id, nodes)

    # Testing the Raft consensus algorithm
    raft_node.start_election()
    raft_node.receive_vote_request(1, 1, 0, 0)
    raft_node.collect_votes()
    raft_node.receive_log_request(1, 1, 0, 0, 0, [])
    raft_node.receive_log_response(1, 1, 0, True)
    raft_node.commit_log_entries()

if __name__ == "__main__":
    main()



'''
class RaftNode:
    def __init__(self):
        # Initialize Raft node

    def on_initialization(self):
        # Perform actions when the node is initialized
        pass

    def on_recovery_from_crash(self):
        # Perform actions when the node recovers from a crash
        pass

    def on_leader_failure_or_election_timeout(self):
        # Handle leader failure or election timeout
        pass

    def on_vote_request_received(self, candidate_id, candidate_term, candidate_log_length, candidate_log_term):
        # Handle receiving a vote request
        pass

    def on_vote_response_received(self, voter_id, term, granted):
        # Handle receiving a vote response
        pass

    def broadcast_message(self, msg):
        # Broadcast a message to all nodes
        pass

    def periodic_task(self):
        # Perform periodic tasks
        pass

    def replicate_log_entry(self, leader_id, follower_id):
        # Replicate log entry from leader to follower
        pass

    def on_log_request_received(self, leader_id, term, prefix_len, prefix_term, leader_commit, suffix):
        # Handle receiving a log request
        pass

    def append_entries(self, prefix_len, leader_commit, suffix):
        # Append entries to log
        pass

    def on_log_response_received(self, follower_id, term, ack, success):
        # Handle receiving a log response
        pass

    def commit_log_entries(self):
        # Commit log entries
        pass

    def handle_leader_commit_acknowledgements(self):
        # Handle acknowledgements for leader committing log entries
        pass

    def client_request(self, client_id, message):
        # Handle client request
        pass

    def client_response(self, client_id, success):
        # Send response to client
        pass

'''