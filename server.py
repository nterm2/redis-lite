import socket
from serializer import serializer
from deserializer import deserializer, RedisException

HOST = "127.0.0.1"
PORT = 6385

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
            command_word = resp_repr.pop(0)

            # Implement PING
            if command_word == 'PING':
                if len(resp_repr) == 0:
                    # Means no parameter has been passed into ping. return pong
                    resp_response = serializer('PONG', use_bulk_str=True)
                elif len(resp_repr) == 1:
                    # A parameter has been passed into ping. return the parameter
                    resp_response = serializer(resp_repr[0], use_bulk_str=True)
                else:
                    # Passed in more than one parameter. Not allowed. Send back an error
                    resp_response = serializer(RedisException("wrong number of arguments for the 'ping' command"))
                conn.sendall(resp_response.encode('utf-8'))
                print(f"Sent: {resp_response}")
            # Implement ECHO
            elif command_word == 'ECHO':
                if len(resp_repr) == 1:
                    # A parameter has been passed into echo. return the parameter
                    resp_response = serializer(resp_repr[0], use_bulk_str=True)
                else:
                    # Multiple or no parameters have been passed into echo. return an error
                    resp_response = serializer(RedisException("ERR wrong number of arguments for command"))

                conn.sendall(resp_response.encode('utf-8'))
                print(f"Sent: {resp_response}")

                
