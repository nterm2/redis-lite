from serializer import serializer
from deserializer import deserializer, RedisException
import datetime, json, os

PERSISTENT_REDIS_FILENAME = "dump.json"
VALID_EXPIRY_COMMANDS = ["EX", "PX", "EXAT", "PXAT"]

class Server:
    def __init__(self, connection):
        self.__connection = connection
        self.__redis_dict = self.__initialise_redis_dict()
        self.__command_word_mapping = {
            "PING": self.__ping,
            "ECHO": self.__echo,
            "SET": self.__r_set,
            "GET": self.__get,
            "EXISTS": self.__exists,
            "DEL": self.__delete,
            "INCR": self.__incr,
            "DECR": self.__decr,
            "LPUSH": self.__lpush,
            "RPUSH":  self.__rpush,
            "SAVE": self.__save
        }
    
    def handle_client(self):
        with self.__connection:
            while True:
                data = self.__connection.recv(1024)
                if not data:
                    break
                self.__resp_data = deserializer(data.decode('utf-8'))
                self.__command_word = self.__resp_data.pop(0)
                try:
                    method_to_execute = self.__command_word_mapping[self.__command_word]
                except:
                    resp_response = serializer(RedisException(f"unknown command {self.__command_word}"))
                else:
                    resp_response = method_to_execute()
                self.__connection.sendall(resp_response.encode("utf-8"))

    def __initialise_redis_dict(self):
        try:
            if os.path.exists(PERSISTENT_REDIS_FILENAME):
                with open(PERSISTENT_REDIS_FILENAME, 'r') as f:
                    return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            print("not found")
            
        return {}
        
    def __ping(self):
        match len(self.__resp_data):
            case 0:
                return serializer('PONG', use_bulk_str=True)
            case 1:
                message = self.__resp_data[0]
                return serializer(message, use_bulk_str=True)
            case _:
                return serializer(RedisException("wrong number of arguments for the 'ping' command"))
            
    def __echo(self):
        if len(self.__resp_data) == 1:
            message = self.__resp_data[0]
            return serializer(message, use_bulk_str=True)
        else:
            return serializer(RedisException("ERR wrong number of arguments for command"))
    
    def __determine_expiry_time(self, expiry_command, timeframe):
        current_time = datetime.datetime.now()
        match expiry_command:
            case "EX":
                return current_time + datetime.timedelta(seconds=timeframe)
            case "PX":
                return current_time + datetime.timedelta(milliseconds=timeframe)
            case "EXAT":
                return datetime.datetime.fromtimestamp(timeframe)
            case "PXAT":
                return datetime.datetime.fromtimestamp(timeframe / 1000)

    def __invalid_set_resp(self, expiry_command, expiry_timeframe):
        # If invalid return a RESP string serialized exception.
        # This is a truthy value. Otherwise return False.
        if (expiry_command not in VALID_EXPIRY_COMMANDS):
            return serializer(RedisException("syntax error"))
        elif (not expiry_timeframe.is_digit()):
            return serializer(RedisException("value is not an integer or out of range"))
        elif (int(expiry_timeframe) < 0):
            return serializer(RedisException("invalid expire time in 'set' command"))
        else:
            return False
        
    def __set_expiring_key(self):
        expiry_command = self.__resp_data[0].upper()
        expiry_timeframe = self.__resp_data[3]
        invalid_resp_exception = self.__invalid_set_resp(expiry_command, expiry_timeframe)
        if (invalid_resp_exception):
            return invalid_resp_exception
        expiry_command = int(expiry_command)
        exact_expiry_time = self.__determine_expiry_time(expiry_command, expiry_timeframe)
        self.__redis_dict[self.__resp_data[0]] = {'data': self.__resp_data[1], 'expires_at': exact_expiry_time}
        return serializer("OK", use_bulk_str=False)
    
    def __r_set(self):
        match len(self.__resp_data):
            case 2:
                self.__redis_dict[self.__resp_data[0]] = {'data': self.__resp_data[1], 'expires_at': None}
                return serializer("OK", use_bulk_str=False)
            case 4:
                return self.__set_expiring_key()
            case _:
                return serializer(RedisException("ERR wrong number of arguments for command"))

    def __get(self):
        if (self.__resp_data == 1):
            key_name = self.__resp_data[0]
            value = self.__redis_dict.get(key_name, None)
            if value and value['expires_at'] and value['expires_at'] <= datetime.datetime.now():
                del self.__resp_data[key_name]
                value = None
            elif value:
                value = value['data']
            return serializer(value)
        else: 
            return serializer(RedisException("ERR wrong number of arguments for command"))
        
    def __exists(self):
        if len(self.__resp_data) > 0:
            redis_lite_dict_keys = list(self.__redis_dict.keys())
            counter = 0
            for potential_key in self.__resp_data:
                if potential_key in redis_lite_dict_keys:
                    counter += 1
            return serializer(counter)
        else:
            return serializer(RedisException("ERR wrong number of arguments for command"))

    def __delete(self):
        if len(self.__resp_data) > 0:
            counter = 0
            for potential_key in self.__resp_data:
                if potential_key in self.__redis_dict.keys():
                    del self.__redis_dict[potential_key]
                else:
                    counter += 1
            return serializer(counter)
        else:
            return serializer(RedisException("ERR wrong number of arguments for command"))  
        
    def __increment_decrement_new_key(self, new_key_name, increment=True):
        self.__redis_dict[new_key_name] = {'data': 0, 'expires_at': None}
        self.__redis_dict[new_key_name]['data'] = self.__redis_dict[new_key_name]['data'] + 1 if increment else self.__redis_dict[new_key_name]['data'] - 1
        return serializer(self.__redis_dict[new_key_name]['data'])

    def __represents_int(self, s):
        s = str(s)
        if s[0] in ('-', '+'):
            return s[1:].isdigit()
        return s.isdigit()    

    def __increment_decrement_existing_key(self, key_name, increment=True):
        try:
            current_value = int(self.__redis_dict[key_name]['data'])
            new_value = current_value + 1 if increment else current_value - 1
            self.__redis_dict[key_name]['data'] = new_value
            return new_value
        except KeyError:
            raise RedisException("ERR no such key")
        except ValueError:
            raise RedisException("ERR value is not an integer")


    def __incr(self):
        if len(self.__resp_data) == 1:
            key_to_increment = self.__resp_data[0]
            if (key_to_increment not in list(self.__redis_dict.keys())):
                return self.__increment_decrement_new_key()
            elif (self.__represents_int(self.__redis_dict[key_to_increment]['data'])):
                return serializer(self.__increment_decrement_existing_key(self.__redis_dict, key_to_increment))
            else:
                return serializer(RedisException("value is not an integer or out of range")) 
        else:
            return serializer(RedisException("ERR wrong number of arguments for command"))  

    def __decr(self):
        if len(self.__resp_data) == 1:
            key_to_decrement = self.__resp_data[0]
            if (key_to_decrement not in list(self.__redis_dict.keys())):
                return self.__increment_decrement_new_key(increment=False)
            elif (not self.__represents_int(self.__redis_dict[key_to_decrement]['data'])):
                return serializer(RedisException("value is not an integer or out of range")) 
            else:
                return serializer(self.__increment_decrement_existing_key(key_to_decrement, increment=False))
        else:
            return serializer(RedisException("ERR wrong number of arguments for command"))  

    def __push_elements(self, elements, key, push_left=True):
        data_value = self.__redis_dict[key]['data']
        if type(data_value) != list:
            return serializer(RedisException("WRONGTYPE Operation against a key holding the wrong kind of value"))
        if push_left:
            elements.reverse()
            [data_value.insert(0, element) for element in elements]
        else:
            [data_value.append(element) for element in elements]
        return serializer(len(data_value))


    def __lpush(self):
        if len(self.__resp_data) > 1:
            if self.__resp_data[0] in list(self.__redis_dict.keys()):
                return self.__push_elements(self.__resp_data[1:], self.__resp_data[0])
            else:
                self.__redis_dict[self.__resp_data[0]] = {'data': [], 'expires_at': None}
                return self.__push_elements(self.__resp_data[1:], self.__resp_data[0])
        else:
            return serializer(RedisException("ERR wrong number of arguments for command"))

    def __rpush(self):
        if len(self.__resp_data) > 1:
            if self.__resp_data[0] in list(self.__redis_dict.keys()):
                return self.__push_elements(self.__resp_data[1:], self.__resp_data[0], push_left=False)
            else:
                self.__redis_dict[self.__resp_data[0]] = {'data': [], 'expires_at': None}
                return self.__push_elements(self.__resp_data[1:], self.__resp_data[0], push_left=False)
        else:
            return serializer(RedisException("ERR wrong number of arguments for command"))

    def __save(self):
        if len(self.__resp_data) == 0:
            try:
                with open("redis_lite_dump.json", 'w') as file:
                    json.dump(self.__redis_dict, file)
                return serializer("OK")
            except IOError as e:
                return serializer(f"ERR {str(e)}")
        else:
            return serializer(RedisException("ERR wrong number of arguments for command"))
