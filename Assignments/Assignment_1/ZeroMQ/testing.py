from message_server import Server
from group import Group
from user import User
import time

def main():
        
    # Create a group
    group = Group("group1", "tcp://localhost:6000", "localhost", 6001)
    group.register_group()

    # Create users
    user1 = User("user1", "tcp://localhost:6000")
    user2 = User("user2", "tcp://localhost:6000")

    # Fetch the list of available groups
    print("Fetching group list:")
    user1.get_group_list()

    # Join the group
    user1.join_group("group1", "tcp://localhost:6001")
    user2.join_group("group1", "tcp://localhost:6001")

    # Send messages
    user1.send_message("group1", "Hello, everyone!")
    time.sleep(1)  # Delay to ensure timestamps are different
    user2.send_message("group1", "Hi, there!")

    # # Fetch messages
    # print("Fetching messages:")
    # user1.get_messages("group1")

    # # Leave the group
    # user1.leave_group("group1")
    # user2.leave_group("group1")

if __name__ == "__main__":
    main()
