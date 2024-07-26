class RedisException(Exception):
    pass

def find_array_end(data):
    """Helper function to find the end index of a nested array"""
    count = 1
    for idx in range(1, len(data)):
        if data[idx].startswith('*'):
            count += int(data[idx][1:])
        elif data[idx][0] in '+-:$':
            count -= 1
        if count == 0:
            return idx + 1
    return len(data)

def parse_simple_string(simple_string: str):
    first_byte = simple_string[0]
    if first_byte == '+':
        # Only include the string itself. remove first byte.
        simple_string = simple_string[1:]
        return str(simple_string)

def parse_simple_error(simple_error: str):
    first_byte = simple_error[0]
    if first_byte == '-':
        simple_error_message = simple_error[1:]
        return RedisException(simple_error_message)
    
def parse_integer(integer: str):
    first_byte = integer[0]  
    if first_byte == ":":
        integer = integer[1:]
        return int(integer)

def parse_bulk_string(length_and_data: list):
    first_byte = length_and_data[0][0]
    if first_byte == '$':
        length_and_data[0] = length_and_data[0][1:]
        bulk_string_length = int(length_and_data[0])
        bulk_string_data = length_and_data[1]
        if len(bulk_string_data) == bulk_string_length:
            return str(bulk_string_data)

def parse_array(num_elements_and_data: list):
    first_byte = num_elements_and_data[0][0]
    # Check that data passed in actually represents an array.
    if first_byte == "*":
        array_length = int(num_elements_and_data[0][1:])
        array_object = []
        i = 1 # Skip over first byte while looping.
        while array_length > 0:
            first_byte = num_elements_and_data[i][0]
            if first_byte == '+':
                array_object.append(parse_simple_string(num_elements_and_data[i]))
                i += 1
            elif first_byte == '-':
                array_object.append(parse_simple_error(num_elements_and_data[i]))
                i += 1
            elif first_byte == ':':
                array_object.append(parse_integer(num_elements_and_data[i]))
                i += 1
            elif first_byte == '$':
                array_object.append(parse_bulk_string(num_elements_and_data[i:i+2]))
                i += 2
                
            array_length -= 1
        return array_object

def deserialize_resp(resp_string: str):
    """Deserialize resp list into their corresponding python objects"""
    resp_list = resp_string.split('\\r\\n')
    resp_list.pop() 
    first_byte = resp_list[0][0]
    if first_byte == "+":
        simple_string = resp_list[0]
        return parse_simple_string(simple_string)
    elif first_byte == "-":
        simple_error = resp_list[0]
        return parse_simple_error(simple_error)
    elif first_byte == ":":
        integer = resp_list[0]
        return parse_integer(integer)
    elif first_byte == "$":
        return parse_bulk_string(resp_list)
    elif first_byte == "*":
        return parse_array(resp_list)

resp_string = r'*1\r\n$4\r\nping\r\n'
deserialized_value = deserialize_resp(resp_string)
print(deserialized_value)