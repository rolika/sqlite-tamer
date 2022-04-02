import unittest
import code.tamer as tamer

class JsonTest(unittest.TestCase):
    """Test database creation from json files"""

    def setUp(self) -> None:
        self._conn = tamer.Tamer.create_from_json("test/sql_create.json", "test/sql_default.json")
    
    def test_table_person(self):
        expected = ("prefix", "lastname", "firstname", "nickname", "sex", "id", "created", "modified")        
        return self.assertEqual(self._conn["person"].get_columns("person"), expected)
    
    def test_table_company(self):
        expected = ("shortname", "fullname", "id", "created", "modified")        
        return self.assertEqual(self._conn["company"].get_columns("company"), expected)
    
    def test_table_contact(self):
        expected = ("person_id", "company_id", "id", "created", "modified")
        return self.assertEqual(self._conn["contact"].get_columns("contact"), expected)


    def tearDown(self):
        for conn in self._conn.values():
            conn.drop()


if __name__ == "__main__":
    unittest.main()
