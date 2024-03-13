class Config:
    # Cluster configuration
    NUM_NODES = 5
    NODE_IPS = ["127.0.0.1", "127.0.0.1", "127.0.0.1", "127.0.0.1", "127.0.0.1"]  # Example IP addresses
    NODE_PORTS = [5000, 5001, 5002, 5003, 5004]  # Example port numbers

    # Timeout configuration
    ELECTION_TIMEOUT_MIN = 0.15  # Minimum election timeout in seconds
    ELECTION_TIMEOUT_MAX = 0.3   # Maximum election timeout in seconds
    HEARTBEAT_INTERVAL = 0.1     # Heartbeat interval in seconds

    # Log configuration
    MAX_LOG_ENTRIES = 1000  # Maximum number of log entries in the log
    LOG_COMPACT_THRESHOLD = 500  # Log compaction threshold (number of entries)
