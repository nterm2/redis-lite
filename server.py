import socket
from serializer import serializer
from deserializer import deserializer

HOST = "127.0.0.1"
PORT = 6382

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.bind((HOST, PORT))
    s.listen()
    print(f"Server listening on {HOST}:{PORT}")
    conn, addr = s.accept()
    with conn:
        print(f"Connected by {addr}")
        while True:
            data = conn.recv(1024)
            if not data:
                break
            resp = data.decode('utf-8')
            resp_repr = deserializer(resp)
            print(f"Received: {resp_repr}")
            if resp_repr == ['ping']:
                resp_response = serializer('PONG')
                print(resp_response)
                conn.sendall(resp_response.encode('utf-8'))
                print(f"Sent: {resp_response}")
            else:
                print(f"Unknown command: {resp}")
