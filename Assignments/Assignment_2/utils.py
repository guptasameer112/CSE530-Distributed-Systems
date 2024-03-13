import socket

def send_message(ip, port, message):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(1)  # Set timeout for socket operation
            s.connect((ip, port))
            s.sendall(message.encode())
    except Exception as e:
        # Handle connection error
        print(f"Error sending message to {ip}:{port}: {e}")

def receive_message(socket):
    try:
        data = socket.recv(1024).decode()
        return data
    except Exception as e:
        # Handle receive error
        print(f"Error receiving message: {e}")
        return None
