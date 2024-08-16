import redis
import unittest

class TestRedisServer(unittest.TestCase):
    def setUp(self):
        self.r = redis.Redis(host="localhost", port=6385, socket_connect_timeout=5, socket_timeout=5, decode_responses=True)

    def test_valid_ping(self):
        self.assertEqual(self.r.ping(), True)

    def test_valid_echo(self):
        self.assertEqual(self.r.echo("hello world"), "hello world")
    
    def test_valid_set_get(self):
        self.assertEqual(self.r.set("name", "hello"), True)
        self.assertEqual(self.r.get("name"), "hello")
    
    def test_invalid_get(self):
        self.assertEqual(self.r.get("abcd"), None)

if __name__ == "__main__":
    unittest.main()
