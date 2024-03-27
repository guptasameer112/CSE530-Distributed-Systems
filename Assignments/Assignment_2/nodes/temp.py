import grpc
import raft_pb2
import raft_pb2_grpc
import time
import random
import sys
import os
import threading
import schedule

from concurrent import futures

MAJORITY = 3
LEASE_DURATION = 10

class RaftNode(raft_pb2_grpc.RaftServicer):
    def __init__(self, selfid):
        self.id = selfid
        self.all_ids = [1,2,3,4,5] # LIST OF ALL IDs
        self.node_addresses = ['localhost:50051', 'localhost:50052', 'localhost:50053', 'localhost:50054', 'localhost:50055']
        # Persistent state
        self.currentTerm = 0
        self.votedFor = None
        self.log = [] # {'term' : term, 'command' : command}
        self.commitLength = 0


        #volatile
        self.currentRole = 'follower'
        self.currentLeader = None
        self.votesReceived = set()  # CHANGE IT!!! ITS a Dict or a set
        self.sentLength = dict() # node_id:length
        self.ackedLength = dict() # node_id:length
        self.db = dict() # key:value

        self.election_timeout = self.calculate_election_timeout()

        self.channel = None
        self.stub = None
        self.start_election_timer()



    def start_election_timer(self):
        election_timeout = random.uniform(5, 10)
        self.election_timer = threading.Timer(election_timeout, self.collect_votes)
        self.election_timer.start()

    def start_heartbeat_timer(self):
        self.heartbeat_timer = threading.Timer(1, self.send_heartbeats) # CHANGE FUNCTION
        self.heartbeat_timer.start()

    def start_lease_timer(self):
        self.lease_start_time = time.time()
        self.lease_timer = threading.Timer(LEASE_DURATION, self.lease_timeout) # CHANGE FUNCTION
        self.lease_timer.start()

    def cancel_election_timer(self):
        if self.election_timer:
            self.election_timer.cancel()

    def cancel_heartbeat_timer(self):
        if self.heartbeat_timer:
            self.heartbeat_timer.cancel()

    def cancel_lease_timer(self):
        if self.lease_timer:
            self.lease_timer.cancel()




    def RequestVote(self, request, context):
        response = raft_pb2.RequestVoteResponse()

        print(self.id, ' has recieved a vote request from', request.candidateId)

        if request.term > self.currentTerm:
            self.currentTerm = request.term
            self.currentRole = 'follower'
            self.votedFor = None

        lastTerm = 0
        if len(self.log) > 0:
            lastTerm = self.log[-1]['term']
        
        # fix list out of index error
        if (request.term == self.currentTerm) and \
            (self.votedFor is None or self.votedFor == request.candidateId) and \
                (request.lastLogTerm > lastTerm or
                 (request.lastLogTerm == lastTerm and request.lastLogIndex >= len(self.log))):
            self.votedFor = request.candidateId
            response.term = self.currentTerm
            response.voteGranted = True
            return response

        response.term = self.currentTerm
        response.voteGranted = False
        return response
    
    def collect_votes(self):
        self.currentTerm += 1
        self.currentRole = 'candidate'
        self.votedFor = self.id
        self.votesReceived.add(self.id)
        
        lastTerm = 0
        if len(self.log) > 0: lastTerm = self.log[-1]['term']

        threads = []

        for address in self.all_ids:
            # Send a RequestVote RPC to each server
            thread = self.voting_async(lastTerm, address)
            threads.append(thread)
            # CANCEL ELECTION TIMER ?

    
    def voting_async(self, lastTerm, address):

        def vote_rpc():
            request = raft_pb2.RequestVoteRequest(
                term=self.currentTerm,
                candidateId=self.id,
                lastLogIndex=len(self.log),
                lastLogTerm=lastTerm
            )
            with grpc.insecure_channel(self.node_addresses[address-1]) as channel:
                try:
                    stub = raft_pb2_grpc.RaftStub(channel)
                    response = stub.RequestVote(request)
                    print("Vote Request RPC sent from", self.id, "to", address)

                except grpc.RpcError as e:
                    print(f"{self.id} Failed RPC error ho gaya : {e}")
                    return

            if self.currentRole == 'candidate' and response.voteGranted and self.currentTerm == response.term:
                print(address ,"voted for", self.id)
                self.votesReceived.add(address)
                if len(self.votesReceived) >= MAJORITY:
                    print("bancho im leader:", self.id)
                    self.currentRole = 'leader'
                    self.currentLeader = self.id
                    # CANCEL ELECTION TIMER ?

                    for addr in self.all_ids:
                        if(addr != self.id):
                            self.sentLength[addr] = len(self.log)
                            self.ackedLength[addr] = 0
                            self.replicateLog(self.id, addr) # FILL THIS FUNCTION!!

            elif response.term > self.currentTerm:
                self.currentTerm = response.term
                self.currentRole = 'follower'
                self.votedFor = None

        thread = threading.Thread(target=vote_rpc)
        thread.start()
        return thread
    
    # THIS REQUEST TO MESSAGE IS COMING FROM THE CLIENT 
    def broadcast_message(self, request):
        log_record = {'command': request, 'term': self.currentTerm}
        self.log.append(log_record)
        
        self.ackedLength[self.id] = len(self.log)
        
        for follower_id in self.all_ids:
            if follower_id != self.id:
                self.replicateLog(self.id, follower_id)

    def ServeClient(self, request, context):
        if self.currentRole == 'leader':
            if request.Request.split()[0] == 'SET':
                self.broadcast_message(request.Request)
                return raft_pb2.ServeClientReply(Data='Entry Updated', LeaderID=self.currentLeader, Success=True)
            elif request.Request.split()[0] == 'GET':
                key = request.Request.split()[1]
                if key in self.db:
                    return raft_pb2.ServeClientReply(Data=self.db[key], LeaderID=self.currentLeader, Success=True)
                else:
                    return raft_pb2.ServeClientReply(Data='Key not found', LeaderID=self.currentLeader, Success=False)

        else:
            return raft_pb2.ServeClientReply(Data=f'Update Leader, LeaderId = {self.currentLeader}', LeaderID=self.currentLeader, Success=False)

    # HEARTBEAT ----> CHANGE/INTERGRATE THIS WITH THE MAIN RUN LOOP
    def periodically(self):
        if self.currentRole == 'leader':
            for follower_id in self.all_ids:
                if follower_id != self.id:
                    self.replicateLog(self.id, follower_id)            

    def replicateLog(self, leaderId, followerId):
        prefix_len = self.sentLength[followerId]
    
        suffix = self.log[prefix_len:]
        
        prefix_term = 0

        if prefix_len > 0:
            prefix_term = self.log[prefix_len - 1]['term']
        # {'term' : term, 'command' : command}
        # Send LogRequest message to follower
            
        message = raft_pb2.LogRequest(
            leaderId=leaderId,
            term=self.currentTerm,
            prefixLen=prefix_len,
            prefixTerm=prefix_term,
            leaderCommit=self.commitLength,
            suffix=suffix
        )
        self.channel = grpc.insecure_channel(self.node_addresses[followerId - 1])
        self.stub = raft_pb2_grpc.RaftStub(self.channel)
        response = self.stub.ProcessLog(message)

        # SLIDE 8/9 BELOW HERE: we are using processing the log and getting the response being used in 8/9

        if (response.term == self.currentTerm) and (self.currentRole == 'leader'):
            if (response.success and response.ack >= self.ackedLength[response.follower]):
                self.sentLength[response.follower] = response.ack
                self.ackedLength[response.follower] = response.ack
                self.CommitLogEntries()
            elif self.sentLength[response.follower] > 0:
                self.sentLength[response.follower] = self.sentLength[response.follower] - 1
                self.replicateLog(self.id, response.follower)
        
        elif response.term > self.currentTerm:
            self.currentTerm = response.term
            self.currentRole = 'follower'
            self.votesReceived = None
            self.ele

    def acks(self, in_len):
        counter = 0
        for addrs in self.all_ids:
            if (self.ackedLength[addrs] >= in_len):
                counter+=1

        return counter

    def CommitLogEntries(self):
        minAcks = 3
        ready = [i for i in range(1, len(self.log)+1) if self.acks(i) >= minAcks]
        if len(ready) != 0 and max(ready) > self.commitLength and self.log[max(ready)-1]['term'] == self.currentTerm:

            for i in range(self.commitLength, max(ready)):
                # DELIVER self.log[i][] TO THE APPLICATION (WRITE TO DUMP FILE) 
                self.db[self.log[i]['command'].split()[1]] = self.log[i]['command'].split()[2]  
                # print(f'DELIVER self.log[{i}][] TO THE APPLICATION (WRITE TO DUMP FILE)')

            self.commitLength = max(ready)
    

    def ProcessLog(self, request, context):
        print(f'Processing log request from {request.leaderId}')
        if request.term > self.currentTerm:
            self.currentTerm = request.term
            self.votedFor = None
            # Cancel election timer

        if request.term == self.currentTerm:
            self.currentRole = 'follower'
            self.currentLeader = request.leaderId
            
        logOk = (len(self.log) >= request.prefixLen and (request.prefixLen == 0 or self.log[request.prefixLen - 1]['term'] == request.prefixTerm))

        if request.term == self.currentTerm and logOk:
            self.AppendEntries(request.prefixLen, request.leaderCommit, request.suffix)
            ack = request.prefixLen + len(request.suffix)
            return raft_pb2.LogResponse(follower = self.id, term=self.currentTerm,ack = ack, success=True)
        else:
            return raft_pb2.LogResponse(follower = self.id, term=self.currentTerm,ack = 0, success=False)
        


    def AppendEntries(self, prefixLen, leaderCommit, suffix):
        if len(suffix) > 0 and len(self.log) > prefixLen:
            index = min(len(self.log), prefixLen+len(suffix)) -1 

            if(self.log[index]['term'] != suffix[index-prefixLen]['term']):
                self.log = self.log[:prefixLen]

        if prefixLen + len(suffix) > len(self.log):
            for i in range(len(self.log)-prefixLen, len(suffix)):
                self.log.append(suffix[i])

        if leaderCommit > self.commitLength:
            for i in range(self.commitLength, leaderCommit):
                # DELIVER self.log[i][] TO THE APPLICATION (WRITE TO DUMP FILE)           
                print(f'DELIVER self.log[{i}][] TO THE APPLICATION (WRITE TO DUMP FILE)')

            self.commitLength = leaderCommit
            
                

def serve():

    # if os.environ.get('https_proxy'):
    #     del os.environ['https_proxy']
    # if os.environ.get('http_proxy'):
    #     del os.environ['http_proxy']
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=100))
    node_obj = RaftNode(int(sys.argv[1]))
    raft_pb2_grpc.add_RaftServicer_to_server(node_obj, server)
    server.add_insecure_port(f'[::]:{sys.argv[2]}')
    server.start()
    # server.wait_for_termination()
    try:
        while True:
            time.sleep(86400)

    except KeyboardInterrupt:
        server.stop(0)
        print("Server stopped")
    # node_obj.start()

if __name__ == '__main__':
    serve()


