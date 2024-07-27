class RedisException(Exception):
    pass

def parse_simple_string(simple_string: str):
    if simple_string[0] == '+':
        return simple_string[1:]

def parse_simple_error(simple_error: str):
    if simple_error[0] == '-':
        return RedisException(simple_error[1:])

def parse_integer(integer: str):
    if integer[0] == ':':
        return int(integer[1:])

def parse_bulk_string(resp_list: list):
    length = int(resp_list.pop(0)[1:])
    if length == -1:
        return None
    bulk_string_data = resp_list.pop(0)
    if len(bulk_string_data) == length:
        return bulk_string_data
    else:
        raise ValueError("Bulk string length mismatch")

def parse_array(resp_list: list):
    array_length = int(resp_list.pop(0)[1:])
    array = []
    for _ in range(array_length):
        array.append(deserialize_resp(resp_list))
    return array

def tokenize(resp: str):
    return resp.split('\r\n')[:-1]

def deserialize_resp(resp_list: list):
    if not resp_list:
        return None
    
    first_byte = resp_list[0][0]
    if first_byte == "+":
        return parse_simple_string(resp_list.pop(0))
    elif first_byte == "-":
        return parse_simple_error(resp_list.pop(0))
    elif first_byte == ":":
        return parse_integer(resp_list.pop(0))
    elif first_byte == "$":
        return parse_bulk_string(resp_list)
    elif first_byte == "*":
        return parse_array(resp_list)
    else:
        raise ValueError(f"Unexpected token: {resp_list[0]}")

# Example usage:
resp_string = "*3\r\n:1\r\n:2\r\n*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n:3\r\n"
tokens = tokenize(resp_string)
print("Tokens:", tokens)
deserialized_value = deserialize_resp(tokens)
print("Deserialized value:", deserialized_value)
