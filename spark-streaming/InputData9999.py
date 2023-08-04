import socket
import time

# Text to send
text = """Hello,
This is a long text
with three lines."""

# Connect to the server (localhost:9999)
client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client_socket.connect(('localhost', 9999))

# Send the text every 2 seconds in a loop
try:
    while True:
        client_socket.sendall(text.encode())
        print("Text sent successfully.")
        time.sleep(2)  # Wait for 2 seconds before sending again
except KeyboardInterrupt:
    print("Sending stopped by user.")

# Close the connection
client_socket.close()
