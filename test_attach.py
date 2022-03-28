import unittest
import sqlite3
import tamer


class AttachTest(unittest.TestCase):
    """Test ataching databases"""

    def setUp(self) -> None:
        self._conn = tamer.Tamer.create_from_json("sql_create.json", "sql_default.json")
        # setup a contact
        person_id = self._conn["kontakt"].insert("szemely", vezeteknev="Doe", keresztnev="John")
        organisation_id = self._conn["kontakt"].insert("szervezet", rovidnev="Example Inc.")
        contact_id = self._conn["kontakt"].insert("kontakt", szemelyazonosito=person_id, szervezetazonosito=organisation_id)
        self._customer_id = self._conn["kontakt"].insert("vevo", kontaktazonosito=contact_id)
        self._responsible_id = self._conn["kontakt"].insert("ajanlatkeszito", kontaktazonosito=contact_id)
        # setup a project
        project_id = self._conn["projekt"].insert("projekt", nev="Main Project", ev=22, szam=314)
        part_id = self._conn["projekt"].insert("munkaresz", projektazonosito=project_id, megnevezes="Part of Project")
        self._trait = self._conn["projekt"].insert("jelleg", munkareszazonosito=part_id, megnevezes="new project")
    
    def test_project(self):
        request_id = self._conn["projekt"].insert("megkereses", jelleg=self._trait, ajanlatkero=self._customer_id, temafelelos=self._responsible_id)
        return self.assertEqual(request_id, 1)
    
    def test_illegal_insert(self):
        self.assertFalse(self._conn["projekt"].insert("megkereses", jelleg=20, ajanlatkero=245, temafelelos=4))
            

    def tearDown(self):
        for conn in self._conn.values():
            conn.drop()


if __name__ == "__main__":
    unittest.main()