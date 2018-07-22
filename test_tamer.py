import unittest
import sqlite3
import tamer

class TamerTest(unittest.TestCase):
    """Testing sqlite-tamer Tamer() class"""

    def setUp(self):
        self.conn = tamer.Tamer()
        self.conn.create("movies", "title", "year", "watched")
        self.conn.insert("movies", title="Star Wars", year=1977, watched=2012)
        self.conn.insert("movies", title="The Matrix", year=1999, watched=50)
        self.conn.insert("movies", title="Avengers", year=2012, watched=2012)
        self.conn.insert("movies", title="The Lord of The Rings", year=2002, watched=2)
        self.conn.insert("movies", title="2012", year=2009, watched=1)
        self.conn.insert("movies", title="2012", year=2012, watched=2012)

    def test_init(self):
        self.assertIsInstance(self.conn, sqlite3.Connection)

    def test_create(self):
        table = self.conn.execute("""SELECT name FROM sqlite_master WHERE type = 'table'""")
        self.assertEqual(next(table)["name"], "movies", "Creating 'movies' failed")

    def test_insert(self):
        rows = self.conn.execute("""SELECT * FROM movies""")
        self.assertEqual(len(tuple(rows)), 6, "Inserting 6 rows failed")

    def test_select_all(self):
        self.assertEqual(len(tuple(self.conn.select("movies"))), 6, "Failed to fetch 6 rows")

    def test_select_rowid(self):
        self.assertEqual(next(self.conn.select("movies", rowid=2))["title"], "The Matrix", "Failed to fetch The Matrix")

    def test_select_or(self):
        self.assertEqual(len(tuple(self.conn.select("movies", title="2012", year=2012, watched=2012))), 4, "Failed to fetch 4 rows")

    def test_select_and(self):
        self.assertEqual(len(tuple(self.conn.select("movies", "AND", title="2012", year=2012, watched=2012))), 1, "Failed to fetch 1 row")

    def test_select_not(self):
        self.assertEqual(len(tuple(self.conn.select("movies", "NOT", watched=1))), 5, "Failed to fetch 5 rows")

    def test_delete_1(self):
        self.conn.delete("movies", "NOT", watched=1)
        self.assertEqual(len(tuple(self.conn.select("movies"))), 1, "Failed to delete 5 rows")

    def test_delete_2(self):
        self.conn.delete("movies", year=2012)
        self.assertEqual(len(tuple(self.conn.select("movies"))), 4, "Failed to delete 2 rows")

    def test_update_1(self):
        self.conn.update("movies", {"watched": 2013}, title="Star Wars")
        self.assertEqual(next(self.conn.select("movies", rowid=1))["watched"], 2013, "Failed to watch Star Wars one more time :-(")

    def test_update_2(self):
        self.conn.update("movies", {"title": "Star Wars: A new hope", "watched": 2013}, logic="AND", title="Star Wars", year=1977)
        self.assertEqual(next(self.conn.select("movies", rowid=1))["title"], "Star Wars: A new hope", "Failed to update to Star Wars: A new hope")

    def test_update_3(self):
        self.conn.update("movies", {"title": "Star Wars: A new hope", "watched": 2013}, logic="AND", title="Star Wars", year=1978)
        self.assertNotEqual(next(self.conn.select("movies", rowid=1))["title"], "Star Wars: A new hope", "Updated the wrong Star Wars movie")

    def test_drop(self):
        self.assertTrue(self.conn.drop("movies"), "Failed to drop 'movies'")

    def tearDown(self):
        self.conn.close()

if __name__ == "__main__":
    unittest.main()
