import unittest
from code.tamer.tamer import Tamer


class AttachTest(unittest.TestCase):
    """Test ataching databases"""

    def setUp(self) -> None:
        self._conn = Tamer.create_from_json("test/sql_create.json", "test/sql_default.json")
        # setup a contact
        self._person_id = self._conn["person"].insert("person", lastname="Doe", firstname="John")
        self._conn["person"].insert("person", lastname="Doe", firstname="Jane")
        self._company_id = self._conn["company"].insert("company", shortname="Example", fullname="Example Inc.")
        self._conn["contact"].insert("contact", person_id=self._person_id, company_id=self._company_id)
    
    def test_contact(self):
        contact = self._conn["contact"].execute("""
        SELECT printf('%s/%s', shortname, firstname)
        FROM contact, person, company
        WHERE person_id = ? AND company_id = ?;
        """, (self._person_id, self._company_id)).fetchone()[0]
        return self.assertEqual("Example/John", contact)            

    def tearDown(self):
        for conn in self._conn.values():
            conn.drop()


if __name__ == "__main__":
    unittest.main()