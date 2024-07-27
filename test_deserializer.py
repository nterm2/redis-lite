import unittest

from deserializer import deserializer, RedisException

class TestDeserializer(unittest.TestCase):
    def test_valid_null(self):
        # test resp strings that are equal to null are deserialized to null
        self.assertEqual(deserializer("$-1\r\n"), None)
        self.assertEqual(deserializer("*-1\r\n"), None)

    def test_valid_simple_string(self):
        # Test deserialization of valid simple strings
        self.assertEqual(deserializer("+OK\r\n"), "OK")

    def test_valid_simple_error(self):
        with self.assertRaises(RedisException) as context:
            raise RedisException("Error message")
        # test deserialization of valid simple errors
        self.assertEqual(deserializer("-Error message\r\n"), context.exception)

    def test_valid_integers(self):
        # test deserialization of resp integers
        self.assertEqual(deserializer(":8\r\n"), 8)
        self.assertEqual(deserializer(":+8\r\n"), 8)
        self.assertEqual(deserializer(":-8\r\n"), -8)

    def test_valid_bulk_string(self):
        # test deserialization of a valid bulk string
        self.assertEqual(deserializer("$0\r\n\r\n"), "")
        self.assertEqual(deserializer("$5\r\nhello\r\n"), "hello")
    
    def test_valid_array(self):
        # test deserialization of valid arrays, and nested arrays
        self.assertEqual(deserializer("*2\r\n$3\r\nget\r\n$3\r\nkey\r\n"), ["get", "key"])
        self.assertEqual(deserializer('*2\r\n$4\r\necho\r\n$11\r\nhello world\r\n'), ["echo", "hello world"])
        self.assertEqual(deserializer("*4\r\n:1\r\n:2\r\n*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n:3\r\n"), [1, 2, ['foo', 'bar'], 3])
    
    def test_invalid_resp(self):
        with self.assertRaises(ValueError) as cm:
            deserializer("#2\r\n$3\r\nget\r\n$3\r\nkey\r\n")
        error_object = cm.exception
        self.assertEqual(str(error_object), "Unexpected token: #2")
    
    def test_invalid_simple_strings(self):
        with self.assertRaises(ValueError) as cm:
            deserializer("+\r\n")     
        exception = cm.exception
        self.assertEqual(str(exception), "RESP data cannot include empty simple strings")  
    
    def test_invalid_simple_errors(self):
        with self.assertRaises(ValueError) as cm:
            deserializer("-\r\n")     
        exception = cm.exception
        self.assertEqual(str(exception), "RESP data cannot include error objects without an error message")  

    def test_invalid_integers(self):
        # Test exception is raised for values that are not integers
        with self.assertRaises(ValueError) as cm:
            deserializer(":0.8\r\n")     
        exception = cm.exception
        self.assertEqual(str(exception), "RESP integer data must not be empty and must be an integer") 
        # Also test for null integers
        with self.assertRaises(ValueError) as cm:
            deserializer(":\r\n")     
        exception = cm.exception
        self.assertEqual(str(exception), "RESP integer data must not be empty and must be an integer") 

    def test_invalid_bulk_strings(self):
        # Test for bulk strings where length of string is not the same as the actual length of the string
        with self.assertRaises(ValueError) as cm:
            deserializer("$8\r\nhello\r\n")     
        exception = cm.exception
        self.assertEqual(str(exception), "Bulk string length mismatch") 
        # Test for bulk strings where the length specified isn't an integer
        with self.assertRaises(ValueError) as cm:
            deserializer("$5.1\r\nhello\r\n")     
        exception = cm.exception
        self.assertEqual(str(exception), "Bulk string length data should be an integer") 

    def test_invalid_array(self):
        # Test exception is raised for array length mismatch
        with self.assertRaises(ValueError) as cm:
            deserializer("*40\r\n$3\r\nget\r\n$3\r\nkey\r\n")     
        exception = cm.exception
        self.assertEqual(str(exception), "Array string length mismatch")
        # Test exception is raised for array length that does not have an integer type
        with self.assertRaises(ValueError) as cm:
            deserializer("*2.5\r\n$3\r\nget\r\n$3\r\nkey\r\n")     
        exception = cm.exception
        self.assertEqual(str(exception), "Array string length data should be an integer")

if __name__ == "__main__":
    unittest.main()