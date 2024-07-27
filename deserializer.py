class RedisException(Exception):
    def __init__(self, message):
        super().__init__(message)
        self.message = message 
    
    def __str__(self):
        return f"RedisException: {self.message}"
    
    def __eq__(self, other):
        if isinstance(other, RedisException):
            return self.message == other.message
        return False

def check_string_valid_integer(string: str):
    """Return boolean as to whether a string represents only an integer value"""
    stripped_int = string.lstrip("+-")
    return stripped_int.isdigit()

def deserializer(resp: str):
    def parse_simple_string(simple_string: str):
        first_byte = simple_string[0]
        if first_byte == '+':
            if len(simple_string) == 1:
                # An empty simple string has been input, which is not allowed.
                raise ValueError("RESP data cannot include empty simple strings")
            return simple_string[1:]

    def parse_simple_error(simple_error: str):
        if simple_error[0] == '-':
            if len(simple_error) == 1:
                # A simple error that does not have an error message has been input, which is not allowed.
                raise ValueError("RESP data cannot include error objects without an error message")             
            return RedisException(simple_error[1:])

    def parse_integer(integer: str):
        if integer[0] == ':':
            # Check that an actual integer has been passed through.
            if check_string_valid_integer(integer[1:]):
                return int(integer[1:])
            else:
                raise ValueError(f"RESP integer data must not be empty and must be an integer")

    def parse_bulk_string(resp_list: list):
        length = resp_list.pop(0)[1:]
        if check_string_valid_integer(length):
            length = int(length)
            if length == -1:
                return None
            bulk_string_data = resp_list.pop(0)
            if len(bulk_string_data) == length:
                return bulk_string_data
            else:
                raise ValueError("Bulk string length mismatch")
        else:
            raise ValueError(f"Bulk string length data should be an integer")

    def parse_array(resp_list: list):
        array_length = resp_list.pop(0)[1:]
        if check_string_valid_integer(array_length):
            array_length = int(array_length)
            array = []
            if array_length == - 1:
                return None
            for _ in range(array_length):
                # If there are no more items to consider within the resp list, there is an issue with the array length in the resp data
                # Raise value error.
                if (len(resp_list) == 0):
                    raise ValueError("Array string length mismatch")
                array.append(deserialize_resp(resp_list))
            return array
        else: 
            raise ValueError("Array string length data should be an integer")           

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
        
    resp_list = tokenize(resp)
    return deserialize_resp(resp_list)