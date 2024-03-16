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
        self.all_ids = [] # LIST OF ALL IDs
        # Persistent state
        self.currentTerm = 0
        self.votedFor = None
        self.log = []
        self.commitLength = 0

        #volatile
        self.currentRole = 'follower'
        self.currentLeader = None
        self.votesReceived = set()  # CHANGE IT!!! ITS a Dict or a set
        self.sentLength = dict() # node_id:length
        self.ackedLength = dict() # node_id:length

        self.election_timeout = self.calculate_election_timeout()

        self.channel = grpc.insecure_channel(address)
        self.stub = raft_pb2_grpc.MarketStub(self.channel)
        self.address = f'{self.get_ip()}:{port}'
        
        # From raft paper
        # Volatile state
        # self.commitIndex = 0
        # self.lastApplied = 0
        # # Volatile state on leaders
        # self.nextIndex = {}
        # self.matchIndex = {}

    def calculate_election_timeout(self):
        # Calculate a random election timeout
        return time.time() + random.uniform(0.15, 0.3)

    def AppendEntries(self, request, context):
        response = raft_pb2.AppendEntriesResponse()

        # Update currentTerm if needed
        if request.term > self.currentTerm:
            self.currentTerm = request.term
            self.votedFor = None  # Reset votedFor since term has changed

        # Rule 1: Reply false if term < currentTerm
        if request.term < self.currentTerm:
            response.term = self.currentTerm
            response.success = False
            return response

        # Rule 2: Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
        if len(self.log) < request.prevLogIndex or \
                (len(self.log) >= request.prevLogIndex and
                 self.log[request.prevLogIndex - 1].term != request.prevLogTerm):
            response.term = self.currentTerm
            response.success = False
            return response

        # Rule 3: If an existing entry conflicts with a new one, delete the existing entry and all that follow it
        if len(self.log) > request.prevLogIndex:
            del self.log[request.prevLogIndex:]

        # Rule 4: Append any new entries not already in the log
        for entry in request.entries:
            if entry.term > self.currentTerm:
                self.currentTerm = entry.term
            self.log.append(entry)

        # Rule 5: If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
        if request.leaderCommit > self.commitIndex:
            self.commitIndex = min(request.leaderCommit, len(self.log))

        response.term = self.currentTerm
        response.success = True
        return response

    def RequestVote(self, request, context):
        response = raft_pb2.RequestVoteResponse()

        #  If the candidate's term is greater than the recipient's current term, the recipient becomes a follower in that term (even if it was a leader in a previous term). 
        if request.term > self.currentTerm:
            self.currentTerm = request.term
            self.currentRole = 'follower'
            self.votedFor = None

        # Rule 2: If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote
        if (request.term == self.currentTerm) and \
            (self.votedFor is None or self.votedFor == request.candidateId) and \
                (request.lastLogTerm > self.log[-1].term or
                 (request.lastLogTerm == self.log[-1].term and request.lastLogIndex >= len(self.log))):
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
                lastLogTerm=self.log[-1].term if self.log else 0
            )
            response = stub.RequestVote(request)

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
                        self.replicate(self.id, addr)

            elif response.term > self.currentTerm:
                self.currentTerm = response.term
                self.currentRole = 'follower'
                self.votedFor = None
                # CANCEL ELECTION TIMER ?

            
            


    
    

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    raft_pb2_grpc.add_RaftServicer_to_server(RaftNode(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()


