from deserializer import RedisException

def serializer(obj):
    """Serializes a python object into its RESP format"""
    def parse_simple_string(string: str):
        if len(string) > 0:
            return f"+{string}\r\n"
        else:
            raise ValueError("RESP data cannot include empty simple strings")

    def parse_simple_errors(simple_error: RedisException):
        if len(simple_error.message) > 0:
            return f"-{simple_error.message}\r\n"
        else:
            raise ValueError("RESP data cannot include error objects without an error message")    

    def parse_integer(integer:int):
        if (isinstance(integer, int)):
            return f":{integer}\r\n"
        else:
            raise ValueError(f"RESP integer data must not be empty and must be an integer")  

    def parse_bulk_string(bulk_string: str | None):
        if (bulk_string == None):
            return "$-1\r\n"
        else:
            length = len(bulk_string)
            return f"${length}\r\n{bulk_string}\r\n"

    def parse_array(array: list | None):
        if (array == None):
            return "*-1\r\n"
        else:
            array_length = len(array)
            array_resp = f"*{array_length}\r\n"

            for _ in range(array_length):
                current_element = array.pop(0)
                array_resp += serialize_python_object(current_element)
            
            return array_resp
        
    def serialize_python_object(obj):
        if (obj == None):
            return parse_bulk_string(obj)
        elif isinstance(obj, str):
            if '\r' in obj or '\n' in obj or len(obj) == 0:
                return parse_bulk_string(obj)
            else:
                return parse_simple_string(obj)
        elif isinstance(obj, RedisException):
            return parse_simple_errors(obj)
        elif isinstance(obj, int):
            return parse_integer(obj)
        elif isinstance(obj, list):
            return parse_array(obj)
        else:
            raise ValueError("Invalid object type")

    return serialize_python_object(obj)
