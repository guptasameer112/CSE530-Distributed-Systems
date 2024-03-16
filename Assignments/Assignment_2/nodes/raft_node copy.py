import sys
import time
import threading
import socket
import random
import utils

class RaftNode:
    def __init__(self, ip, port):
        self.ip = ip
        self.all_ips = [] #fill with all IPs
        self.state = 'FOLLOWER'
        self.followers = [] #fill with all follower IPs 
        self.port = port
        self.current_term = 0
        self.voted_for = None
        self.commit_index = 0
        self.leader_id = None
        self.log = []
        
        self.election_timeout = self.calculate_election_timeout() 

        # self.last_applied = 0
        # self.next_index = {}
        # self.match_index = {}
        
    def start(self):
        # Start necessary threads and initialize the node
        threading.Thread(target=self.run).start()

    def run(self):
        # Main logic for the node
        while True:
            if self.state == 'FOLLOWER':
                self.handle_messages()
                if time.time() > self.election_timeout:
                    self.start_election()
            elif self.state == 'CANDIDATE':
                self.handle_messages()
                if time.time() > self.election_timeout:
                    self.start_election()
            elif self.state == 'LEADER':
                self.send_heartbeat()
                time.sleep(1)  # Adjust sleep time as needed

    def send_heartbeat(self):
        for follower_ip, follower_port in self.followers:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.settimeout(1)  # Set timeout for socket operation
                    s.connect((follower_ip, follower_port))
                    msg = "AppendEntriesRPC"  # Placeholder for actual AppendEntries message
                    s.sendall(msg.encode())
            except Exception as e:
                # Handle connection error
                print(f"Error sending heartbeat to {follower_ip}:{follower_port}: {e}")

    def receive_messages(self):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind((self.ip, self.port))
                s.listen()

                print(f"Node {self.node_id} is listening for messages...")
                while True:
                    conn, addr = s.accept()
                    with conn:
                        data = conn.recv(1024).decode()
                        if data:
                            print(f"Received message from {addr}: {data}")
                            # Process the received message, depending on the protocol
        except Exception as e:
            print(f"Error receiving messages: {e}")


    def handle_messages(self):
        while True:
            try:
                # Receive message from other nodes
                message = self.receive_message()

                # Process different types of messages
                if message:
                    message_type, data = message.split(" ", 1)
                    
                    if message_type == "AppendEntriesRPC":
                        self.handle_append_entries_rpc(data)
                    elif message_type == "RequestVoteRPC":
                        self.handle_request_vote_rpc(data)
                    # Add more conditions to handle other types of messages
                    
            except Exception as e:
                # Handle any errors that occur during message handling
                print(f"Error handling messages: {e}")

    def handle_append_entries(self, message):
        try:
            # Parse the message data
            term, leader_id, prev_log_index, prev_log_term, entries, leader_commit = message.split(" ")
            term = int(term)
            leader_id = int(leader_id)
            prev_log_index = int(prev_log_index)
            prev_log_term = int(prev_log_term)
            leader_commit = int(leader_commit)
            
            # If the received term is less than the current term, reject the RPC and return False
            if term < self.current_term:
                return False
            
            # If the received term is greater than the current term, update the current term and convert to follower state
            if term > self.current_term:
                self.current_term = term
                self.state = 'FOLLOWER'
                self.voted_for = -1
            
            # Check if the log contains an entry at prevLogIndex whose term matches prevLogTerm
            if prev_log_index >= 0:
                existing_entry_term, _ = self.commit_log.get_entry(prev_log_index)
                if existing_entry_term != prev_log_term:
                    return False
            
            # Append any new entries not already in the log
            if entries:
                entries = entries.split(",")
                for entry in entries:
                    term, command = entry.split(" ")
                    self.commit_log.log(int(term), command)
            
            # If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
            if leader_commit > self.commit_index:
                self.commit_index = min(leader_commit, self.commit_log.last_index)
            
            # Respond to the leader
            return True  # Indicate successful handling of the RPC
        except Exception as e:
            # Handle any errors that occur during message processing
            print(f"Error handling AppendEntries RPC: {e}")
            return False  # Indicate failure in handling the RPC

    def handle_request_vote(self, message):
        try:
            # Parse the message data
            term, candidate_id, last_log_index, last_log_term = message.split(" ")
            term = int(term)
            candidate_id = int(candidate_id)
            last_log_index = int(last_log_index)
            last_log_term = int(last_log_term)
            
            # If the received term is less than the current term, reject the RPC and return False
            if term < self.current_term:
                return False
            
            # If the received term is greater than the current term, update the current term and convert to follower state
            if term > self.current_term:
                self.current_term = term
                self.state = 'FOLLOWER'
                self.voted_for = -1
            
            # Check if the candidate's log is at least as up-to-date as the receiver's log
            if last_log_term < self.commit_log.last_term or \
                (last_log_term == self.commit_log.last_term and last_log_index < self.commit_log.last_index):
                return False
            
            # If the receiver has not voted in this term and the candidate's log is up-to-date, grant the vote
            if self.voted_for == -1 or self.voted_for == candidate_id:
                self.voted_for = candidate_id
                return True  # Grant the vote
            
            return False  # Deny the vote if already voted for another candidate in this term
        except Exception as e:
            # Handle any errors that occur during message processing
            print(f"Error handling RequestVote RPC: {e}")
            return False  # Indicate failure in handling the RPC


    def start_election(self):
    # Start an election process
    
    # Increment the current term and transition to candidate state
        self.current_term += 1
        self.state = 'CANDIDATE'
        
        # Vote for self
        self.voted_for = self.server_index
        
        # Reset the election timeout
        self.set_election_timeout()
        
        # Send RequestVote RPCs to other nodes
        for i in range(len(self.partitions)):
            for j in range(len(self.partitions[i])):
                if i == self.cluster_index and j != self.server_index:
                    ip, port = self.conns[i][j]
                    message = f"{self.current_term} {self.server_index} {self.commit_log.last_index} {self.commit_log.last_term}"
                    msg = f"REQUEST-VOTE {message}"
                    utils.send_and_recv(msg, ip, port)

    def process_vote_request(self, message):
        # Process a vote request
        term, candidate_id, last_log_index, last_log_term = message.split()
        term = int(term)
        candidate_id = int(candidate_id)
        last_log_index = int(last_log_index)
        last_log_term = int(last_log_term)
        
        # Check if the candidate's term is higher than the current term
        if term > self.current_term:
            self.step_down(term)
        
        # Check if this is a valid vote request based on the candidate's log
        if term == self.current_term and \
            (self.voted_for == -1 or self.voted_for == candidate_id) and \
            (last_log_term > self.commit_log.last_term or \
            (last_log_term == self.commit_log.last_term and last_log_index >= self.commit_log.last_index)):
            
            # Vote for the candidate and reset the election timeout
            self.voted_for = candidate_id
            self.set_election_timeout()
            
            # Send a vote reply
            response = f"VOTE-REPLY {self.current_term} {self.server_index}"
            ip, port = self.conns[self.cluster_index][candidate_id]
            utils.send_and_recv(response, ip, port)


    def process_vote_reply(self, message):
        # Process a vote reply
        term, voter_id = message.split()
        term = int(term)
        voter_id = int(voter_id)
        
        # Check if the received term is higher than the current term
        if term > self.current_term:
            self.step_down(term)
        
        # If the node is still a candidate and the reply matches the current term,
        # consider the vote
        if term == self.current_term and self.state == 'CANDIDATE':
            self.votes.add(voter_id)
            
            # Check if the node has received votes from the majority of the cluster
            if len(self.votes) > len(self.partitions[self.cluster_index]) / 2:
                self.state = 'LEADER'
                self.leader_id = self.server_index
                print(f"Node {self.server_index} became the leader for term {self.current_term}.")

    def step_down(self):
        # Step down from leader to follower
        
        # Reset the state to follower
        self.state = 'FOLLOWER'
        
        # Clear the voted_for field
        self.voted_for = -1
        
        # Reset the election timeout
        self.set_election_timeout()
        
        print(f"{self.ip}:{self.port} stepped down from leader to follower.")

    def commit_entries(self):
        # Commit log entries once replicated
        
        # Get the index and term of the last committed entry
        last_committed_index, last_committed_term = self.commit_log.get_last_index_term()
        
        # Loop through the log entries starting from the last committed index
        for index in range(last_committed_index + 1, self.commit_index + 1):
            # Get the term and command of the log entry
            term, command = self.commit_log.read_logs_start_end(index, index)[0]
            
            # Check if the term matches the last committed term
            if term == last_committed_term:
                # Apply the command to the state machine
                self.apply_log_entry(command)
                
                # Update the last committed index and term
                self.commit_log.last_index = index
                self.commit_log.last_term = term

    def append_entries_to_followers(self):
        # Send AppendEntries RPC to followers
        
        # Iterate through each follower node
        for follower_node in range(len(self.partitions[self.cluster_index])):
            # Skip sending append entries to self (leader)
            if follower_node == self.server_index:
                continue
            
            # Get the IP address and port of the follower node
            follower_ip, follower_port = self.conns[self.cluster_index][follower_node]
            
            # Prepare the AppendEntries RPC message
            message = f"AppendEntries {self.current_term} {self.leader_id}"
            
            # Send the AppendEntries RPC message to the follower node
            response = utils.send_message(follower_ip, follower_port, message)
            
            # Process the response from the follower node
            if response:
                # Assuming the response indicates success or failure
                if response == "Success":
                    print(f"AppendEntries RPC successful for node {follower_node}")
                else:
                    print(f"AppendEntries RPC failed for node {follower_node}: {response}")
            else:
                print(f"No response received from node {follower_node}")


    def replicate_log_entry(self, entry):
    # Replicate a log entry to followers
    
    # Iterate through each follower node
        for follower_node in range(len(self.partitions[self.cluster_index])):
            # Skip replicating entry to self (leader)
            if follower_node == self.server_index:
                continue
            
            # Get the IP address and port of the follower node
            follower_ip, follower_port = self.conns[self.cluster_index][follower_node]
            
            # Prepare the log replication message
            message = f"REPLICATE-LOG {entry}"
            
            # Send the log replication message to the follower node
            response = utils.send_message(follower_ip, follower_port, message)
            
            # Process the response from the follower node
            if response == "Success":
                # Handle successful replication
                print(f"Log entry replicated successfully to follower at {follower_ip}:{follower_port}")
            else:
                # Handle replication failure
                print(f"Failed to replicate log entry to follower at {follower_ip}:{follower_port}: {response}")

    def apply_log_entry(self, entry):
        # Apply a log entry to the state machine
        
        # Split the log entry to extract the command and its arguments
        parts = entry.split()
        command = parts[0]
        args = parts[1:]
        
        # Execute the corresponding action based on the command
        if command == "SET":
            key, value = args
            self.ht.set(key, value)
            print(f"Set key '{key}' to value '{value}'")
        elif command == "DELETE":
            key = args[0]
            # Implement deletion logic if needed
            print(f"Deleted key '{key}'")
        else:
            print(f"Unknown command: {command}. Skipping log entry.")

    def calculate_election_timeout(self):
        # Calculate a random election timeout
        return time.time() + random.uniform(0.15, 0.3)

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python raft_node.py <ip> <port>")
        sys.exit(1)

    ip = sys.argv[1]
    port = int(sys.argv[2])

    node = RaftNode(ip, port)
    node.start()
