import socket

server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.bind(('0.0.0.0', 9999))
server_socket.listen(1)

print("Socket server listening on localhost as same port")

while True:
    conn, addr = server_socket.accept()
    data = conn.recv(10240000000)
    print(f"Received data: {data.decode()}")
    # conn.close()

print("Socket closed")
