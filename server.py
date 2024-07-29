import socket
from serializer import serializer
from deserializer import deserializer, RedisException

HOST = "127.0.0.1"
PORT = 6385

redis_lite_dict = {}

def handle_client(conn):
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
            if command_word.upper() == 'PING':
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
            elif command_word.upper() == 'ECHO':
                if len(resp_repr) == 1:
                    # A parameter has been passed into echo. return the parameter
                    resp_response = serializer(resp_repr[0], use_bulk_str=True)
                else:
                    # Multiple or no parameters have been passed into echo. return an error
                    resp_response = serializer(RedisException("ERR wrong number of arguments for command"))
                conn.sendall(resp_response.encode('utf-8'))
                print(f"Sent: {resp_response}")
            # Implement SET
            elif command_word.upper() == 'SET':
                if len(resp_repr) == 2:
                    # two parameters have been passed as expected. set key value pair
                    redis_lite_dict[resp_repr[0]] = resp_repr[1]
                    resp_response = serializer("OK", use_bulk_str=False)
                elif len(resp_repr) < 2:
                    resp_response = serializer(RedisException("ERR wrong number of arguments for command"))
                else:
                    resp_response = serializer(RedisException("syntax error"))
                conn.sendall(resp_response.encode('utf-8'))
                print(f"Sent: {resp_response}")
            # Implement GET
            elif command_word.upper() == 'GET':
                if len(resp_repr) == 1:
                    try:
                        value = redis_lite_dict[resp_repr[0]]
                    except KeyError:
                        resp_response = serializer(None)   
                    else:
                        resp_response = serializer(value)                
                else:
                    resp_response = serializer(RedisException("ERR wrong number of arguments for command"))
                conn.sendall(resp_response.encode('utf-8'))
            else:
                resp_response = serializer(RedisException(f"unknown command {command_word}"))
                conn.sendall(resp_response.encode('utf-8'))
            print(f"Sent: {resp_response}")

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.bind((HOST, PORT))
    s.listen()
    print(f"Server listening on {HOST}:{PORT}")
    while True:
        conn, addr = s.accept()
        handle_client(conn)
