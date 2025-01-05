import socket
from threading import Thread
from server import Server

HOST = "127.0.0.1"
PORT = 6398

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.bind((HOST, PORT))
    s.listen()
    print(f"Server listening on {HOST}:{PORT}")
    while True:
        connection, address = s.accept()
        server = Server(connection=connection)
        Thread(target=server.handle_client).start()
