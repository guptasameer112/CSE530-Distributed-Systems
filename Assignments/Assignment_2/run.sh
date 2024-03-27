# run files nodes/raft_node_1.py, nodes/raft_node_2.py, nodes/raft_node_3.py, nodes/raft_node_4.py, nodes/raft_node_5.py in sepraate terminals

function handle_interrupt {
    echo "Keyboard interrupt detected. Stopping execution."
    # Add cleanup commands if necessary

    # Kill all python processes
    pkill -f "python nodes/raft_node.py"

    exit 1
}

# Trap SIGINT signal (Ctrl+C) and call handle_interrupt function
trap handle_interrupt SIGINT

# Run the following commands in separate terminals



python nodes/raft_node.py 1 50051 &
python nodes/raft_node.py 2 50052 &
python nodes/raft_node.py 3 50053 &
python nodes/raft_node.py 4 50054 &
python nodes/raft_node.py 5 50055 &

wait