import unittest
import tamer

class JsonTest(unittest.TestCase):
    """Test database creation from json files"""

    def setUp(self) -> None:
        self._conn = tamer.Tamer.create_from_json("sql_create.json", "sql_default.json", "db/")
    
    def test_1(self):
        return True

if __name__ == "__main__":
    unittest.main()
