import asyncio
import websockets
import socket

async def handle_websocket(websocket, path):
    spark_host = 'localhost'
    spark_port = 9999

    spark_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    spark_socket.connect((spark_host, spark_port))

    async for message in websocket:
        print(f"Received message: {message}")

        # Forward the received data to the Spark service
        spark_socket.sendall(message.encode())

start_server = websockets.serve(handle_websocket, 'localhost', 9998)

asyncio.get_event_loop().run_until_complete(start_server)
asyncio.get_event_loop().run_forever()
