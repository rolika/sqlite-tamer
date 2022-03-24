import unittest
import tamer

class JsonTest(unittest.TestCase):
    """Test database creation from json files"""

    def setUp(self) -> None:
        self._conn = tamer.Tamer.create_from_json("sql_create.json", "sql_default.json")
    
    def test_table_kontakt(self):
        expected = ('szemely', 'szervezet', 'kontakt', 'cim', 'telefon', 'email', 'vevo', 'szallito', 'gyarto', 'ajanlatkeszito')
        return self.assertEqual(self._conn["kontakt"].get_tables(), expected)
    
    def test_table_projekt(self):
        expected = ('projekt', 'munkaresz', 'hely', 'jelleg', 'megkereses', 'ajanlat')
        print(self._conn["projekt"].get_tables())
        return self.assertEqual(self._conn["projekt"].get_tables(), expected)

    def tearDown(self):
        for conn in self._conn.values():
            conn.drop()


if __name__ == "__main__":
    unittest.main()
