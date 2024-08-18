import socket
import datetime
from threading import Thread
from serializer import serializer
from deserializer import deserializer, RedisException

HOST = "127.0.0.1"
PORT = 6389

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
                    redis_lite_dict[resp_repr[0]] = {'data': resp_repr[1], 'expires_at': None}
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
                                    current_time = datetime.datetime.now()
                                    timeframe = int(timeframe)
                                    if expiry_command == "EX":
                                        expiry_date = current_time + datetime.timedelta(seconds=timeframe)
                                    elif expiry_command == "PX":
                                        expiry_date = current_time + datetime.timedelta(milliseconds=timeframe)
                                    elif expiry_command == "EXAT":
                                        expiry_date = datetime.datetime.fromtimestamp(timeframe)
                                    elif expiry_command == "PXAT":
                                        expiry_date = datetime.datetime.fromtimestamp(timeframe / 1000)
                                    redis_lite_dict[resp_repr[0]] = {'data': resp_repr[1], 'expires_at': expiry_date}
                                    resp_response = serializer("OK", use_bulk_str=False)
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
                    if value and value['expires_at'] and value['expires_at'] <= datetime.datetime.now():
                        del redis_lite_dict[resp_repr[0]]
                        value = None # key has expired, at which we delete the key
                    elif value:
                        value = value['data'] # key exists and is either doesn't have a timestamp or has not been expired yet, at which we can access the data.
                    resp_response = serializer(value)
                else:
                    resp_response = serializer(RedisException("ERR wrong number of arguments for command"))
                conn.sendall(resp_response.encode('utf-8'))
            # Implement EXISTS 
            elif command_word.upper() == 'EXISTS':
                if len(resp_repr) > 0:
                    redis_lite_dict_keys = list(redis_lite_dict.keys())
                    counter = 0
                    for potential_key in resp_repr:
                        print(potential_key)
                        if potential_key in redis_lite_dict_keys:
                            counter += 1
                    resp_response = serializer(counter)
                else:
                    resp_response = serializer(RedisException("ERR wrong number of arguments for command"))
                conn.sendall(resp_response.encode('utf-8'))
            # Implement DEL
            elif command_word.upper() == 'DEL':
                if len(resp_repr) > 0:
                    redis_lite_dict_keys = list(redis_lite_dict.keys())
                    counter = 0
                    for potential_key in resp_repr:
                        try:
                            del redis_lite_dict[potential_key]
                        except:
                            pass 
                        else:
                            counter += 1
                    resp_response = serializer(counter)
                else:
                    resp_response = serializer(RedisException("ERR wrong number of arguments for command"))  
                conn.sendall(resp_response.encode('utf-8'))              
            # Implement INCR 
            elif command_word.upper() == 'INCR':
                pass 
            # Implement DECR
            elif command_word.upper() == 'DECR':
                pass 
            # Implement LPUSH
            elif command_word.upper() == 'LPUSH':
                pass 
            # Implement RPUSH
            elif command_word.upper() == 'RPUSH':
                pass 
            # Implement SAVE 
            elif command_word.upper() == 'SAVE':
                pass
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
