import unittest
from serializer import serializer
from deserializer import RedisException

class TestDeserializer(unittest.TestCase):
    def test_valid_null(self):
        # Test python none objects serialize to appropiate resp representations
        # for null
        self.assertEqual(serializer(None), r"$-1\r\n")
    
    def test_valid_simple_string(self):
        # Test that python strings qualifying for simple strings return 
        # a resp simple string
        self.assertEqual(serializer("OK"), r"+OK\r\n")
    
    def test_valid_simple_error(self):
        # Test that redisexceptions are coverted to their corresponding
        # simple error resp strings.
        with self.assertRaises(RedisException) as context:
            raise RedisException("Error message")
        self.assertEqual(serializer(context.exception), r"-Error message\r\n")
    
    def test_valid_integers(self):
        # Test that integers are converted to their corresponding
        # resp strings 
        self.assertEqual(serializer(1), r":1\r\n")
        self.assertEqual(serializer(+1), r":1\r\n")
        self.assertEqual(serializer(-1), r":-1\r\n")

    def test_valid_bulk_string(self):
        self.assertEqual(serializer(""), r"$0\r\n\r\n")
        self.assertEqual(serializer("\r\n"), '$2\\r\\n\r\n\\r\\n')
    
    def test_valid_array(self):
        self.assertEqual(serializer([1, 2, 3]), r"*3\r\n:1\r\n:2\r\n:3\r\n")
        self.assertEqual(serializer([1, 2, "a"]), r"*3\r\n:1\r\n:2\r\n+a\r\n")
        self.assertEqual(serializer([1, [2, 3, 'a']]), r"*2\r\n:1\r\n*3\r\n:2\r\n:3\r\n+a\r\n")
    
    def test_invalid_python_obj(self):
        with self.assertRaises(ValueError) as cm:
            serializer({"name": "joe"})
        self.assertEqual(str((cm.exception)), "Invalid object type")
    
    def test_empty_array(self):
        self.assertEqual(serializer([]), r'*0\r\n')
    
    def test_invalid_array(self):
        with self.assertRaises(ValueError) as cm:
            serializer([1, 2, 3, {"name": "joe"}])
        self.assertEqual(str((cm.exception)), "Invalid object type")
        
if __name__ == "__main__":
    unittest.main()