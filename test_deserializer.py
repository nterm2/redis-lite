import unittest

from deserializer import deserializer, RedisException


class TestDeserializer(unittest.TestCase):
    def test_null(self):
        # test resp strings that are equal to null are deserialized to null
        self.assertEqual(deserializer("$-1\r\n"), None)
        self.assertEqual(deserializer("*-1\r\n"), None)

    def test_simple_string(self):
        # Test deserialization of valid simple strings
        self.assertEqual(deserializer("+OK\r\n"), "OK")

    def test_simple_error(self):
        with self.assertRaises(RedisException) as context:
            raise RedisException("Error message")
        # test deserialization of valid simple errors
        self.assertEqual(deserializer("-Error message\r\n"), context.exception)




if __name__ == "__main__":
    unittest.main()