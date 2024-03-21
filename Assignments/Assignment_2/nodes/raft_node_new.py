import grpc
import raft_pb2
import raft_pb2_grpc
import time
import random

from concurrent import futures

MAJORITY = 3

class RaftNode(raft_pb2_grpc.RaftServicer):
    def __init__(self, address, port, selfid):
        self.id = selfid
        self.all_ids = [0,1,2,3,4,5] # LIST OF ALL IDs
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

        self.election_timeout = self.calculate_election_timeout()

        self.channel = grpc.insecure_channel(address)
        self.stub = raft_pb2_grpc.RaftStub(self.channel)
        self.address = f'{self.get_ip()}:{port}'

    def start(self):
        while True:
            if self.currentRole == 'follower':
                if time.time() > self.election_timeout:
                    self.currentRole = 'candidate'
                    self.election_timeout = self.calculate_election_timeout()
            elif self.currentRole == 'candidate':
                self.collect_votes()
                if time.time() > self.election_timeout:
                    self.election_timeout = self.calculate_election_timeout()
            elif self.currentRole == 'leader':
                self.periodically(self.id)
                # Start heartbeat timer
        

    def calculate_election_timeout(self):
        # Calculate a random election timeout between 5 and 10 seconds
        return time.time() + random.randint(5, 10)

    def RequestVote(self, request, context):
        response = raft_pb2.RequestVoteResponse()

        if request.term > self.currentTerm:
            self.currentTerm = request['term']
            self.currentRole = 'follower'
            self.votedFor = None

            
        if (request.term == self.currentTerm) and \
            (self.votedFor is None or self.votedFor == request.candidateId) and \
                (request.lastLogTerm > self.log[-1]['term'] or
                 (request.lastLogTerm == self.log[-1]['term'] and request.lastLogIndex >= len(self.log))):
            self.votedFor = request.candidateId
            response.term = self.currentTerm
            response.voteGranted = True
            return response

        response.term = self.currentTerm
        response.voteGranted = False
        return response
    

    def collect_votes(self):
        for address in self.all_ids:
            # Send a RequestVote RPC to each server
            request = raft_pb2.RequestVoteRequest(
                term=self.currentTerm,
                candidateId=self.id,
                lastLogIndex=len(self.log),
                lastLogTerm=self.log[-1]['term'] if self.log else 0
            )
            response = self.stub.RequestVote(request)

            if self.currentRole == 'candidate' and response.voteGranted and self.currentTerm == response.term:
                self.votesReceived.add(address)
                if len(self.votesReceived) >= MAJORITY:
                    self.currentRole = 'leader'
                    self.currentLeader = self.id
                    # CANCEL ELECTION TIMER ?

                for addr in self.all_ids:
                    if(addr != address):
                        self.sentLength[addr] = len(self.log)
                        self.ackedLength[addr] = 0
                        self.replicateLog(self.id, addr) # FILL THIS FUNCTION!!

            elif response.term > self.currentTerm:
                self.currentTerm = response.term
                self.currentRole = 'follower'
                self.votedFor = None
                # CANCEL ELECTION TIMER ?

    # THIS REQUEST TO MESSAGE IS COMING FROM THE CLIENT 
    def broadcast_message(self, request):
        if self.currentRole == 'leader':

            log_record = {'command': request, 'term': self.currentTerm}
            self.log.append(log_record)
            
            self.ackedLength[self.id] = len(self.log)
            
            for follower_id in self.all_ids:
                if follower_id != self.id:
                    self.replicateLog(self.id, follower_id)
        else:
            self.forward_to_client(self.currentLeader)  # sFINISH THIS FUNCION: send back the (message?) to the client if the node isnt a leader

    def ServeClient(self, request, context):
        if self.currentRole == 'leader':
            if request.Request.split()[0] == 'SET':
                self.broadcast_message(request.Request)
                return raft_pb2.ServeClientReply(Data='Entry Updated', LeaderID=self.currentLeader, Success=True)
            elif request.Request.split()[0] == 'GET':
                # SEARCH FOR THE Key IN THE DB
                pass

        else:
            return raft_pb2.ServeClientReply(Data=f'Update Leader, LeaderId = {self.currentLeader}', LeaderID=self.currentLeader, Success=False)

        

#         message ServeClientArgs {
#   string Request = 1;
# }

# message ServeClientReply {
#   string Data = 1;
#   string LeaderID = 2;
#   bool Success = 3;
# }



    # HEARTBEAT ----> CHANGE/INTERGRATE THIS WITH THE MAIN RUN LOOP
    def periodically(self, node_id):
        if self.currentRole == 'leader':
            for follower_id in self.nodes - {node_id}:
                self.replicateLog(node_id, follower_id)            

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
            currentTerm=self.currentTerm,
            prefixLen=prefix_len,
            prefixTerm=prefix_term,
            commitLength=self.commitLength,
            suffix=suffix
        )

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
            # CANCEL ELECTION TIMER

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
                continue

            self.commitLength = max(ready)
    

    def ProcessLog(self, request, context):
        if request.term > self.currentTerm:
            self.currentTerm = request.term
            self.votedFor = None
            # Cancel election timer

        if request.term == self.currentTerm:
            self.currentRole = 'follower'
            self.currentLeader = request.leaderId
            
        logOk = (len(self.log) >= request.prefixLen and (self.log[request.prefixLen - 1]['term'] == request.prefixTerm or request.prefixLen == 0))

        if request.term == self.currentTerm and logOk:
            self.AppendEntries(request.prefixLen, request.commitLength, request.suffix)
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
                continue

            self.commitLength = leaderCommit
            
                

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    raft_pb2_grpc.add_RaftServicer_to_server(RaftNode(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()


