import socket
from threading import Thread
from serializer import serializer
from deserializer import deserializer, RedisException

HOST = "127.0.0.1"
PORT = 6388

redis_lite_dict = {}

def handle_client(conn, addr):
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
                    resp_response = serializer('PONG', use_bulk_str=True)
                elif len(resp_repr) == 1:
                    resp_response = serializer(resp_repr[0], use_bulk_str=True)
                else:
                    resp_response = serializer(RedisException("wrong number of arguments for the 'ping' command"))
                conn.sendall(resp_response.encode('utf-8'))
            # Implement ECHO
            elif command_word.upper() == 'ECHO':
                if len(resp_repr) == 1:
                    resp_response = serializer(resp_repr[0], use_bulk_str=True)
                else:
                    resp_response = serializer(RedisException("ERR wrong number of arguments for command"))
                conn.sendall(resp_response.encode('utf-8'))
            # Implement SET
            elif command_word.upper() == 'SET':
                if len(resp_repr) == 2:
                    redis_lite_dict[resp_repr[0]] = resp_repr[1]
                    resp_response = serializer("OK", use_bulk_str=False)
                elif len(resp_repr) == 4:
                    # Implement expiring keys
                    expiry_command = resp_repr[2].upper()
                    valid_expiry_commands = ["EX", "PX", "EXAT", "PXAT"]
                    timeframe = resp_repr[3]
                    if expiry_command in valid_expiry_commands:
                        try:
                            int(float(timeframe))
                        except:
                            resp_response = serializer(RedisException("value is not an integer or out of range"))
                        else:
                            if int(float(timeframe)) > 0:
                                if timeframe.isdigit():
                                    if expiry_command == "EX":
                                        pass 
                                    elif expiry_command == "PX":
                                        pass 
                                    elif expiry_command == "EXAT":
                                        pass
                                    elif expiry_command == "PXAT":
                                        pass
                                else:
                                    # the timeframe entered is not an integer
                                    resp_response = serializer(RedisException("value is not an integer or out of range"))
                            else:
                                # the timeframe is not valid. can't have a time that is less than or equal to zero
                                resp_response = serializer(RedisException("invalid expire time in 'set' command"))

                    else:
                        # Invalid expiry command
                        resp_response = serializer(RedisException("syntax error"))
                else:
                    resp_response = serializer(RedisException("ERR wrong number of arguments for command"))
                conn.sendall(resp_response.encode('utf-8'))
            # Implement GET
            elif command_word.upper() == 'GET':
                if len(resp_repr) == 1:
                    value = redis_lite_dict.get(resp_repr[0], None)
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
        # Handle each client in a new thread
        Thread(target=handle_client, args=(conn, addr)).start()
