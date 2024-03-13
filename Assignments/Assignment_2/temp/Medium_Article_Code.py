class Raft:
    def __init__(self, ip, port, partitions):
        self.ip = ip
        self.port = port
        self.ht = HashTable()
        self.commit_log = CommitLog(file=f"commit-log-{self.ip}-{self.port}.txt")
        self.partitions = eval(partitions)
        self.conns = [[None]*len(self.partitions[i]) for i in range(len(self.partitions))]
        self.cluster_index = -1
        self.server_index = -1

        # Initialize commit log file
        commit_logfile = Path(self.commit_log.file)
        commit_logfile.touch(exist_ok=True)

        for i in range(len(self.partitions)):
            cluster = self.partitions[i]
            
            for j in range(len(cluster)):
                ip, port = cluster[j].split(':')
                port = int(port)
                
                if (ip, port) == (self.ip, self.port):
                    self.cluster_index = i
                    self.server_index = j
                    
                else: 
                    self.conns[i][j] = (ip, port)
        
        self.current_term = 1
        self.voted_for = -1
        self.votes = set()
        
        u = len(self.partitions[self.cluster_index])
        
        self.state = 'FOLLOWER' if len(self.partitions[self.cluster_index]) > 1 else 'LEADER'
        self.leader_id = -1
        self.commit_index = 0
        self.next_indices = [0]*u
        self.match_indices = [-1]*u
        self.election_period_ms = randint(1000, 5000)
        self.rpc_period_ms = 3000
        self.election_timeout = -1
        
        print("Ready....")


class HashTable:
    def __init__(self):
        self.map = {}
        self.lock = Lock()

    def set(self, key, value, req_id):
        with self.lock:
            if key not in self.map or self.map[key][1] < req_id:
                self.map[key] = (value, req_id)
                return 1
            return -1
    
    def get_value(self, key):
        with self.lock:
            if key in self.map:
                return self.map[key][0]
            return None
        

class CommitLog:
    def __init__(self, file='commit-log.txt'):
        self.file = file
        self.lock = Lock()
        self.last_term = 0
        self.last_index = -1
        
    def get_last_index_term(self):
        with self.lock:
            return self.last_index, self.last_term
        
    def log(self, term, command):
        # Append the term and command to file along with timestamp
        with self.lock:
            with open(self.file, 'a') as f:
                now = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
                message = f"{now},{term},{command}"
                f.write(f"{message}\n")
                self.last_term = term
                self.last_index += 1

            return self.last_index, self.last_term
                
    def log_replace(self, term, commands, start):
        # Replace or Append multiple commands starting at 'start' index line number in file
        index = 0
        i = 0
        with self.lock:
            with open(self.file, 'r+') as f:
                if len(commands) > 0:
                    while i < len(commands):
                        if index >= start:
                            command = commands[i]
                            i += 1
                            now = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
                            message = f"{now},{term},{command}"
                            f.write(f"{message}\n")
                            
                            if index > self.last_index:
                                self.last_term = term
                                self.last_index = index
                        
                        index += 1
                    
                    # Truncate all lines coming after last command.
                    f.truncate()
        
            return self.last_index, self.last_term
    
    def read_log(self):
        # Return in memory array of term and command
        with self.lock:
            output = []
            with open(self.file, 'r') as f:
                for line in f:
                    _, term, command = line.strip().split(",")
                    output += [(term, command)]
            
            return output
        
    def read_logs_start_end(self, start, end=None):
        # Return in memory array of term and command between start and end indices
        with self.lock:
            output = []
            index = 0
            with open(self.file, 'r') as f:
                for line in f:
                    if index >= start:
                        _, term, command = line.strip().split(",")
                        output += [(term, command)]
                    
                    index += 1
                    if end and index > end:
                        break
            
            return output
        
def run_thread(fn, args):
    my_thread = Thread(target=fn, args=args)
    my_thread.daemon = True
    my_thread.start()
    return my_thread

def wait_for_server_startup(ip, port):
    while True:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.connect((str(ip), int(port)))
            return sock
                
        except Exception as e:
            traceback.print_exc(limit=1000)

def send_and_recv_no_retry(msg, ip, port, timeout=-1):
    # Could not connect possible reasons:
    # 1. Server is not ready
    # 2. Server is busy and not responding
    # 3. Server crashed and not responding
    
    conn = wait_for_server_startup(ip, port)
    resp = None
    
    try:
        conn.sendall(msg.encode())
        
        if timeout > 0:
            ready = select.select([conn], [], [], timeout)
            if ready[0]:
                resp = conn.recv(2048).decode()
        else:
            resp = conn.recv(2048).decode()
                
    except Exception as e:
        traceback.print_exc(limit=1000)
        # The server crashed but it is still not marked in current node
    
    conn.close()
    return resp
            
def send_and_recv(msg, ip, port, res=None, timeout=-1):
    resp = None
    # Could not connect possible reasons:
    # 1. Server is not ready
    # 2. Server is busy and not responding
    # 3. Server crashed and not responding
    
    while True:
        resp = send_and_recv_no_retry(msg, ip, port, timeout)
        
        if resp:
            break
            
    if res is not None:
        res.put(resp)
        
    return resp

def init(self):
    # set initial election timeout
    self.set_election_timeout()
    
    # Check for election timeout in the background
    utils.run_thread(fn=self.on_election_timeout, args=())
    
    # Sync logs or send heartbeats from leader to all servers in the background
    utils.run_thread(fn=self.leader_send_append_entries, args=())


def set_election_timeout(self, timeout=None):
    # Reset this whenever previous timeout expires and starts a new election
    if timeout:
        self.election_timeout = timeout
    else:
        self.election_timeout = time.time() + randint(self.election_period_ms, 2*self.election_period_ms)/1000.0

def on_election_timeout(self):
    while True:
        # Check everytime that state is either FOLLOWER or CANDIDATE before sending
        # vote requests.
        
        # The possibilities in this path are:
        # 1. Requestor sends requests, receives replies and becomes leader
        # 2. Requestor sends requests, receives replies and becomes follower again, repeat on election timeout
        if time.time() > self.election_timeout and \
            (self.state == 'FOLLOWER' or self.state == 'CANDIDATE'):
            
            print("Timeout....") 
            self.set_election_timeout() 
            self.start_election() 
                    
def start_election(self):
    print("Starting election...")
    
    # At the start of election, set state to CANDIDATE and increment term
    # also vote for self.
    self.state = 'CANDIDATE'
    self.voted_for = self.server_index
    
    self.current_term += 1
    self.votes.add(self.server_index)
    
    # Send vote requests in parallel
    threads = []
    for j in range(len(self.partitions[self.cluster_index])):
        if j != self.server_index:
           t =  utils.run_thread(fn=self.request_vote, args=(j,))
           threads += [t]
    
    # Wait for completion of request flow
    for t in threads:
        t.join()
    
    return True

def request_vote(self, server):
    # Get last index and term from commit log
    last_index, last_term = self.commit_log.get_last_index_term()
    
    while True:
        # Retry on timeout
        print(f"Requesting vote from {server}...")

        # Check if state if still CANDIDATE
        if self.state == 'CANDIDATE' and time.time() < self.election_timeout:
            ip, port = self.conns[self.cluster_index][server]

            msg = f"VOTE-REQ {self.server_index} {self.current_term} \
                {last_term} {last_index}"
            
            resp = utils.send_and_recv_no_retry(msg, ip, port, timeout=self.rpc_period_ms/1000.0)

            # If timeout happens resp returns None, so it won't go inside this condition
            if resp:
                vote_rep = re.match(
                    '^VOTE-REP ([0-9]+) ([0-9\-]+) ([0-9\-]+)$', resp)

                if vote_rep:
                    server, curr_term, voted_for = vote_rep.groups()

                    server = int(server)
                    curr_term = int(curr_term)
                    voted_for = int(voted_for)

                    self.process_vote_reply(server, curr_term, voted_for)
                    break
        else:
            break

def process_vote_request(self, server, term, last_term, last_index):
    print(f"Processing vote request from {server} {term}...")
    
    if term > self.current_term:
        # Requestor term is higher hence update
        self.step_down(term)

    # Get last index and term from log
    self_last_index, self_last_term = self.commit_log.get_last_index_term()
    
    # Vote for requestor only if requestor term is equal to self term
    # and self has either voted for no one yet or has voted for same requestor (can happen during failure/timeout and retry)
    # and the last term of requestor is greater 
    # or if they are equal then the last index of requestor should be greater or equal.
    # This is to ensure that only vote for all requestors who have updated logs.
    if term == self.current_term \
        and (self.voted_for == server or self.voted_for == -1) \
        and (last_term > self_last_term or
             (last_term == self_last_term and last_index >= self_last_index)):
        
        self.voted_for = server
        self.state = 'FOLLOWER'
        self.set_election_timeout()
        
    return f"VOTE-REP {self.server_index} {self.current_term} {self.voted_for}"

def step_down(self, term):
    print(f"Stepping down...")
    
    # Revert to follower state
    self.current_term = term
    self.state = 'FOLLOWER'
    self.voted_for = -1
    self.set_election_timeout()

def process_vote_reply(self, server, term, voted_for):
    print(f"Processing vote reply from {server} {term}...")
    
    # It is not possible to have term < self.current_term because during vote request
    # the server will update its term to match requestor term if requestor term is higher
    if term > self.current_term:
        # Requestor term is lower hence update
        self.step_down(term)
    
    if term == self.current_term and self.state == 'CANDIDATE':
        if voted_for == self.server_index:
            self.votes.add(server)
        
        # Convert to leader if received votes from majority
        if len(self.votes) > len(self.partitions[self.cluster_index])/2.0:
            self.state = 'LEADER'
            self.leader_id = self.server_index
            
            print(f"{self.cluster_index}-{self.server_index} became leader")
            print(f"{self.votes}-{self.current_term}")

def leader_send_append_entries(self):
    print(f"Sending append entries from leader...")
    
    while True:
        # Check everytime if it is leader before sending append queries
        if self.state == 'LEADER':
            self.append_entries()
            
            # Commit entry after it has been replicated
            last_index, _ = self.commit_log.get_last_index_term()
            self.commit_index = last_index
                    
def append_entries(self):
    res = Queue()
        
    for j in range(len(self.partitions[self.cluster_index])):
        if j != self.server_index:
            # Send append entries requests in parallel
            utils.run_thread(fn=self.send_append_entries_request, args=(j,res,))
    
    if len(self.partitions[self.cluster_index]) > 1:        
        cnts = 0
    
        while True:
            # Wait for servers to respond
            res.get(block=True)
            cnts += 1
            # Once we get reply from majority of servers, then return
            # and don't wait for remaining servers
            # Exclude self
            if cnts > (len(self.partitions[self.cluster_index])/2.0)-1:
                return
    else:
        return
    

def send_append_entries_request(self, server, res=None):
    print(f"Sending append entries to {server}...")
    
    # Fetch previous index and previous term for log matching
    prev_idx = self.next_indices[server]-1
    
    # Get all logs from prev_idx onwards, because all logs after prev_idx will be
    # used to replicate to server
    log_slice = self.commit_log.read_logs_start_end(prev_idx)
    
    if prev_idx == -1:
        prev_term = 0
    else:
        if len(log_slice) > 0:
            prev_term = log_slice[0][0]
            log_slice = log_slice[1:] if len(log_slice) > 1 else []
        else:
            prev_term = 0
            log_slice = []
    
    msg = f"APPEND-REQ {self.server_index} {self.current_term} {prev_idx} {prev_term} {str(log_slice)} {self.commit_index}"
    
    while True:
        if self.state == 'LEADER':
            ip, port = self.conns[self.cluster_index][server]
                
            resp = \
                utils.send_and_recv_no_retry(msg, ip, port,
                                             timeout=self.rpc_period_ms/1000.0)  
            
            # If timeout happens resp returns None, so it won't go inside this condition
            if resp:
                append_rep = re.match('^APPEND-REP ([0-9]+) ([0-9\-]+) ([0-9]+) ([0-9\-]+)$', resp)
                
                if append_rep:
                    server, curr_term, flag, index = append_rep.groups()
                    server = int(server)
                    curr_term = int(curr_term)
                    flag = int(flag)
                    success = True if flag == 1 else False
                    index = int(index)
                    
                    self.process_append_reply(server, curr_term, success, index)
                    break
        else:
            break
    
    if res:
      res.put('ok')


def process_append_requests(self, server, term, prev_idx, prev_term, logs, commit_index):
    print(f"Processing append request from {server} {term}...")
    
    # Follower/Candidate received vote reply, reset election timeout
    self.set_election_timeout()
    
    flag, index = 0, 0
    
    # If term < self.current_term then the append request came from an old leader
    # and we should not take action in that case.
    if term > self.current_term:
        # Most likely term == self.current_term, if this server participated 
        # in voting rounds. If server was down during voting rounds and previous append requests, then term > self.current_term
        # and we should update its term 
        self.step_down(term)
        
    if term == self.current_term:
        # Request came from current leader
        self.leader_id = server
        
        # Check if the term corresponding to the prev_idx matches with that of the leader
        self_logs = self.commit_log.read_logs_start_end(prev_idx, prev_idx) if prev_idx != -1 else []
        
        # Even with retries, this is idempotent
        success = prev_idx == -1 or (len(self_logs) > 0 and self_logs[0][0] == prev_term)
        
        if success:
            # On retry, we will overwrite the same logs
            last_index, last_term = self.commit_log.get_last_index_term()
            
            if len(logs) > 0 and last_term == logs[-1][0] and last_index == self.commit_index:
                # Check if last term in self log matches leader log and last index in self log matches commit index
                # Then this is a retry and will avoid overwriting the logs
                index = self.commit_index
            else:
                index = self.store_entries(prev_idx, logs)
            
        flag = 1 if success else 0
    
    return f"APPEND-REP {self.server_index} {self.current_term} {flag} {index}"

def process_append_reply(self, server, term, success, index):
    print(f"Processing append reply from {server} {term}...")
    
    # It cannot be possible that term < self.current_term because at the time of append request, 
    # all servers will be updated to current term
    
    if term > self.current_term:
        # This could be an old leader which become active and sent
        # append requests but on receiving higher term, it will revert
        # back to follower state.
        self.step_down(term)
        
    if self.state == 'LEADER' and term == self.current_term:
        if success:
            # If server log repaired successfully from leader then increment next index
            self.next_indices[server] = index+1
        else:
            # If server log could not be repaired successfully, then retry with 1 index less lower than current next index
            # Process repeats until we find a matching index and term on server
            self.next_indices[server] = max(0, self.next_indices[server]-1)
            self.send_append_entries_request(server)


def process_append_reply(self, server, term, success, index):
      print(f"Processing append reply from {server} {term}...")
      
      # It cannot be possible that term < self.current_term because at the time of append request, 
      # all servers will be updated to current term
      
      if term > self.current_term:
          # This could be an old leader which become active and sent
          # append requests but on receiving higher term, it will revert
          # back to follower state.
          self.step_down(term)


