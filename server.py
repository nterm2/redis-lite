import socket
import datetime
from threading import Thread
from serializer import serializer
from deserializer import deserializer, RedisException

HOST = "127.0.0.1"
PORT = 6384

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
                if len(resp_repr) == 1:
                    redis_lite_dict_keys = list(redis_lite_dict.keys())
                    key_to_increment = resp_repr[0]
                    if key_to_increment in redis_lite_dict_keys:
                        # Check whether the key's value is a string that can be represented as an integer, an integer, or otherwise (at which an error will be raised.)
                        if isinstance(redis_lite_dict[key_to_increment]["data"], int):
                            # Dealing with an integer
                            redis_lite_dict[key_to_increment]["data"] += 1
                            resp_response = serializer(redis_lite_dict[key_to_increment]["data"])
                        else:
                            # Potentially dealing with a string integer or an errenous value.
                            try:
                                data_to_increment = redis_lite_dict[key_to_increment]['data']
                                valid_int_string = data_to_increment.replace('-', '')
                                # if the string is negative, remove the - sign as isdigit will not work with it.
                                valid_int_string = valid_int_string.isdigit()
                            except:
                                resp_response = serializer(RedisException("value is not an integer or out of range"))  
                            else:
                                if valid_int_string:
                                    redis_lite_dict[key_to_increment]['data'] = str(int(data_to_increment) + 1)
                                    incremented_value = redis_lite_dict[key_to_increment]['data']
                                    resp_response = serializer(incremented_value)
                                else:
                                    resp_response = serializer(RedisException("value is not an integer or out of range")) 
                    else:
                        # Key doesn't exist, at which we create a new key value pair with an intial value of 0, and then inccrement it by 1
                        redis_lite_dict[resp_repr[0]] = {'data': 0, 'expires_at': None}
                        redis_lite_dict[resp_repr[0]]['data'] += 1             
                        resp_response = serializer(redis_lite_dict[resp_repr[0]]['data'])
                else:
                    resp_response = serializer(RedisException("ERR wrong number of arguments for command"))  
                conn.sendall(resp_response.encode('utf-8')) 

            # Implement DECR
            elif command_word.upper() == 'DECR':
                if len(resp_repr) == 1:
                    redis_lite_dict_keys = list(redis_lite_dict.keys())
                    key_to_decrement = resp_repr[0]
                    if key_to_decrement in redis_lite_dict_keys:
                        # Check whether the key's value is a string that can be represented as an integer, an integer, or otherwise (at which an error will be raised.)
                        if isinstance(redis_lite_dict[key_to_decrement]["data"], int):
                            # Dealing with an integer
                            redis_lite_dict[key_to_decrement]["data"] -= 1
                            resp_response = serializer(redis_lite_dict[key_to_decrement]["data"])
                        else:
                            # Potentially dealing with a string integer or an errenous value.
                            try:
                                data_to_decrement = redis_lite_dict[key_to_decrement]['data']
                                valid_int_string = data_to_decrement.replace('-', '')
                                # if the string is negative, remove the - sign as isdigit will not work with it.
                                valid_int_string = valid_int_string.isdigit()
                            except:
                                resp_response = serializer(RedisException("value is not an integer or out of range"))  
                            else:
                                if valid_int_string:
                                    redis_lite_dict[key_to_decrement]['data'] = str(int(data_to_decrement) - 1)
                                    decremented_value = redis_lite_dict[key_to_decrement]['data']
                                    resp_response = serializer(decremented_value)
                                else:
                                    resp_response = serializer(RedisException("value is not an integer or out of range")) 
                    else:
                        # Key doesn't exist, at which we create a new key value pair with an intial value of 0, and then inccrement it by 1
                        redis_lite_dict[resp_repr[0]] = {'data': 0, 'expires_at': None}
                        redis_lite_dict[resp_repr[0]]['data'] -= 1             
                        resp_response = serializer(redis_lite_dict[resp_repr[0]]['data'])
                else:
                    resp_response = serializer(RedisException("ERR wrong number of arguments for command"))  
                conn.sendall(resp_response.encode('utf-8')) 

            # Implement LPUSH
            elif command_word.upper() == 'LPUSH':
                if len(resp_repr) > 1:
                    key_exists = resp_repr[0] in list(redis_lite_dict.keys())
                    if key_exists:
                        value = redis_lite_dict[resp_repr[0]]['data']
                        if type(value) == list:
                            elements_to_add = resp_repr[1:]
                            elements_to_add.reverse()
                            for element in elements_to_add:
                                value.insert(0, element)
                            resp_response = serializer(len(value))
                        else:
                            resp_response = serializer(RedisException("WRONGTYPE Operation against a key holding the wrong kind of value"))
                    else:
                        redis_lite_dict[resp_repr[0]] = {'data': [], 'expires_at': None}
                        value = redis_lite_dict[resp_repr[0]]['data']
                        elements_to_add = resp_repr[1:]
                        elements_to_add.reverse()
                        for element in elements_to_add:
                            value.insert(0, element)
                        resp_response = serializer(len(value))
                else:
                    resp_response = serializer(RedisException("ERR wrong number of arguments for command"))  
                conn.sendall(resp_response.encode('utf-8')) 

            # Implement RPUSH
            elif command_word.upper() == 'RPUSH':
                if len(resp_repr) > 1:
                    key_exists = resp_repr[0] in list(redis_lite_dict.keys())
                    if key_exists:
                        value = redis_lite_dict[resp_repr[0]]['data']
                        if isinstance(value, list):
                            elements_to_add = resp_repr[1:]
                            for element in elements_to_add:
                                value.append(element)
                            resp_response = serializer(len(value))
                        else:
                            resp_response = serializer(RedisException("WRONGTYPE Operation against a key holding the wrong kind of value"))
                    else:
                        redis_lite_dict[resp_repr[0]] = {'data': [], 'expires_at': None}
                        value = redis_lite_dict[resp_repr[0]]['data']
                        elements_to_add = resp_repr[1:]
                        for element in elements_to_add:
                            value.append(element)
                        resp_response = serializer(len(value))
                else:
                    resp_response = serializer(RedisException("ERR wrong number of arguments for command"))
                
                # Ensure resp_response is always defined
                conn.sendall(resp_response.encode('utf-8'))

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
