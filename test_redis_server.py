import redis
import unittest
import time 

class TestRedisServer(unittest.TestCase):
    def setUp(self):
        self.r = redis.Redis(host="localhost", port=6384, socket_connect_timeout=5, socket_timeout=5, decode_responses=True)

    def test_valid_ping(self):
        self.assertEqual(self.r.ping(), True)

    def test_valid_echo(self):
        self.assertEqual(self.r.echo("hello world"), "hello world")
    
    def test_valid_set_get(self):
        self.assertEqual(self.r.set("name", "hello"), True)
        self.assertEqual(self.r.get("name"), "hello")
    
    def test_invalid_get(self):
        self.assertEqual(self.r.get("abcd"), None)
    
    def test_set_get_ex(self):
        self.assertEqual(self.r.set(name="name", value="Bob", ex=5), True)
        time.sleep(1)
        self.assertEqual(self.r.get("name"), "Bob")
        time.sleep(4)
        self.assertEqual(self.r.get("name"), None)

    def test_set_get_px(self):
        self.assertEqual(self.r.set(name="name", value="Bob", px=5), True)
        time.sleep(1 / 1000)
        self.assertEqual(self.r.get("name"), "Bob")
        time.sleep(4 / 1000)
        self.assertEqual(self.r.get("name"), None)

    def test_set_get_exat(self):
        unix_time_in_5_seconds = int(time.time()) + 5
        self.assertEqual(self.r.set(name="name", value="Bob", exat=unix_time_in_5_seconds), True)
        time.sleep(1)
        self.assertEqual(self.r.get("name"), "Bob")
        time.sleep(4)
        self.assertEqual(self.r.get("name"), None)

    def test_set_get_pxat(self):
        unix_time_in_5000_milliseconds = int(time.time() * 1000) + 5000
        self.assertEqual(self.r.set(name="name", value="Bob", pxat=unix_time_in_5000_milliseconds), True)
        time.sleep(1000 / 1000)
        self.assertEqual(self.r.get("name"), "Bob")
        time.sleep(4000 / 1000)
        self.assertEqual(self.r.get("name"), None)
    
    def test_exists(self):
        self.r.set("name", "bob")
        self.r.set("age", 12)
        self.r.set("height", 1.81)
        self.assertEqual(self.r.exists("name", "name", "name"), 3)
        self.assertEqual(self.r.exists("name", "height", "width"), 2)
        self.assertEqual(self.r.exists("width", "depth", "breadth"), 0)

    def test_del(self):
        self.r.set("name", "bob")
        self.r.set("age", 12)
        self.r.set("height", 1.81)
        self.assertEqual(self.r.delete("name", "name", "name"), 1)
        self.assertEqual(self.r.delete("age", "height", "nickname"), 2)
        self.assertEqual(self.r.delete("name", "name", "name"), 0)
    
    def test_incr(self):
        self.r.delete('weight') # Delete the key if it already exists
        self.r.set("age", 12)
        self.r.set("height", "-12")
        self.assertEqual(self.r.execute_command("INCR age"), "13")
        self.assertEqual(self.r.execute_command("INCR height"), "-11")
        self.assertEqual(self.r.execute_command("INCR weight"), 1)

if __name__ == "__main__":
    unittest.main()
