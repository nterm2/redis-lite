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
    





if __name__ == "__main__":
    unittest.main()