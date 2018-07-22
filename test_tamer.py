# test-specific imports
import unittest

# project-specific imports
import sqlite3
import tamer

class TamerTest(unittest.TestCase):
    """Testing sqlite_tamer module's functions"""

    def setUp(self):
        self.con = tamer.init()
        tamer.create(self.con, "movies", "title", "year", "viewed")
        tamer.insert(self.con, "movies", title="Star Wars", year=1977, viewed=2012)
        tamer.insert(self.con, "movies", title="The Matrix", year=1999, viewed=50)
        tamer.insert(self.con, "movies", title="Avengers", year=2012, viewed=2012)
        tamer.insert(self.con, "movies", title="The Lord of The Rings", year=2002, viewed=2)
        tamer.insert(self.con, "movies", title="2012", year=2009, viewed=1)        
        tamer.insert(self.con, "movies", title="2012", year=2012, viewed=2012)

    def test_init(self):
        self.assertIsInstance(self.con, sqlite3.Connection)

    def test_create(self):        
        self.assertTrue(tamer.create(self.con, "movies", "title", "year", "viewed"),
                        "Create table failed")

    def test_insert(self):
        tamer.create(self.con, "movies", "title", "year", "viewed")
        self.assertIsNotNone(tamer.insert(self.con, "movies", title="Star Wars", year=1977, viewed=2012), "Insert row failed")

    def test_select_all(self):        
        self.assertEqual(len(tuple(tamer.select(self.con, "movies"))), 6, "Failed to fetch 6 rows")

    def test_select_rowid(self):
        self.assertEqual(next(tamer.select(self.con, "movies", rowid=2))["title"], "The Matrix", "Failed to fetch The Matrix")

    def test_select_or(self):
        self.assertEqual(len(tuple(tamer.select(self.con, "movies", title="2012", year=2012, viewed=2012))), 4, "Failed to fetch 4 rows")

    def test_select_and(self):
        self.assertEqual(len(tuple(tamer.select(self.con, "movies", "AND", title="2012", year=2012, viewed=2012))), 1, "Failed to fetch 1 row")

    def test_select_not(self):
        self.assertEqual(len(tuple(tamer.select(self.con, "movies", "NOT", viewed=1))), 5, "Failed to fetch 5 rows")

    def test_delete_1(self):
        tamer.delete(self.con, "movies", "NOT", viewed=1)
        self.assertEqual(len(tuple(tamer.select(self.con, "movies"))), 1, "Failed to delete 5 rows")

    def test_delete_2(self):
        tamer.delete(self.con, "movies", year=2012)
        self.assertEqual(len(tuple(tamer.select(self.con, "movies"))), 4, "Failed to delete 2 rows")

    def test_update_1(self):
        tamer.update(self.con, "movies", {"viewed": 2013}, title="Star Wars")
        self.assertEqual(next(tamer.select(self.con, "movies", rowid=1))["viewed"], 2013, "Failed to watch Star Wars one more time :-(")

    def test_update_2(self):
        tamer.update(self.con, "movies", {"title": "Star Wars: A new hope", "viewed": 2013}, logic="AND", title="Star Wars", year=1977)
        self.assertEqual(next(tamer.select(self.con, "movies", rowid=1))["title"], "Star Wars: A new hope", "Failed to update to Star Wars: A new hope")
    
    def test_update_3(self):
        tamer.update(self.con, "movies", {"title": "Star Wars: A new hope", "viewed": 2013}, logic="AND", title="Star Wars", year=1978)
        self.assertNotEqual(next(tamer.select(self.con, "movies", rowid=1))["title"], "Star Wars: A new hope", "Updated the wrong Star Wars movie")

    def tearDown(self):
        self.con.close()

if __name__ == "__main__":
    unittest.main()
