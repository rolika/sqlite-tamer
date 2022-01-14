import unittest
import sqlite3
import tamer

class TamerTest(unittest.TestCase):
    """Testing sqlite-tamer Tamer() class"""

    def setUp(self):
        self.conn = tamer.Tamer("movie.db")
        self.conn.create("movies", "title", "year", "watched", rowid="INTEGER PRIMARY KEY")
        self.conn.insert("movies", title="Star Wars", year=1977, watched=2012)
        self.conn.insert("movies", title="The Matrix", year=1999, watched=50)
        self.conn.insert("movies", title="Avengers", year=2012, watched=2012)
        self.conn.insert("movies", title="The Lord of The Rings", year=2002, watched=2)
        self.conn.insert("movies", title="2012", year=2009, watched=1)
        self.conn.insert("movies", title="2012", year=2012, watched=2012)

    def test_init(self):
        self.assertIsInstance(self.conn, sqlite3.Connection)

    def test_create(self):
        self.assertEqual(self.conn.get_tables(), ("movies",), "Creating 'movies' failed")

    def test_insert(self):
        self.assertEqual(len(self.conn.execute("""SELECT * FROM movies""").fetchall()), 6, "Inserting 6 rows failed")

    def test_select_all(self):
        self.assertEqual(len(self.conn.select("movies").fetchall()), 6, "Failed to fetch 6 rows")

    def test_select_columns(self):
        self.assertEqual(len(self.conn.select("movies", "title", "year").fetchall()), 6, "Failed to fetch specified columns")

    def test_select_rowid(self):
        self.assertEqual(self.conn.select("movies", rowid=2).fetchone()["title"], "The Matrix", "Failed to fetch The Matrix")

    def test_select_or(self):
        self.assertEqual(len(self.conn.select("movies", title="2012", year=2012, watched=2012).fetchall()), 4, "Failed to fetch 4 rows")

    def test_select_and(self):
        self.assertEqual(len(self.conn.select("movies", logic="AND", title="2012", year=2012, watched=2012).fetchall()), 1, "Failed to fetch 1 row")

    def test_select_not(self):
        self.assertEqual(len(self.conn.select("movies", logic="NOT", watched=1).fetchall()), 5, "Failed to fetch 5 rows")

    def test_select_distinct(self):
        self.assertEqual(len(self.conn.select("movies", "title", distinct=True, title="2012").fetchall()), 1, "there should be only one")

    def test_delete_1(self):
        self.conn.delete("movies", logic="NOT", watched=1)
        self.assertEqual(len(self.conn.select("movies").fetchall()), 1, "Failed to delete 5 rows")

    def test_delete_2(self):
        self.conn.delete("movies", year=2012)
        self.assertEqual(len(self.conn.select("movies").fetchall()), 4, "Failed to delete 2 rows")

    def test_update_1(self):
        watched = next(self.conn.select("movies", "watched", logic="AND", title="Star Wars", year=1977))["watched"]
        watched += 1
        self.conn.update("movies", {"watched": watched}, title="Star Wars")
        self.assertEqual(self.conn.select("movies", rowid=1).fetchone()["watched"], 2013, "Failed to watch Star Wars one more time :-(")

    def test_update_2(self):
        self.conn.update("movies", {"title": "Star Wars: A new hope", "watched": 2013}, logic="AND", title="Star Wars", year=1977)
        self.assertEqual(self.conn.select("movies", rowid=1).fetchone()["title"], "Star Wars: A new hope", "Failed to update to Star Wars: A new hope")

    def test_update_3(self):
        self.conn.update("movies", {"title": "Star Wars: A new hope", "watched": 2013}, logic="AND", title="Star Wars", year=1978)
        self.assertNotEqual(self.conn.select("movies", rowid=1).fetchone()["title"], "Star Wars: A new hope", "Updated the wrong Star Wars movie")

    def test_drop_table(self):
        self.conn.drop("movies")
        self.assertFalse(self.conn.get_tables(), "Failed to drop 'movies'")

    def test_drop_column_1(self):
        self.conn.drop("movies", "watched")
        self.assertEqual(self.conn.get_columns("movies"), ("title", "year", "rowid"), "Failed to drop 'watched' column")

    def test_drop_column_2(self):
        self.conn.drop("movies", "watches")
        self.assertNotEqual(self.conn.get_columns("movies"), ("title", "year", "rowid"), "Failed to maintain 'watched' column")

    def test_rename(self):
        self.conn.rename("movies", "films")
        self.assertEqual(self.conn.get_tables(), ("films",), "Renaming 'movies' failed")

    def test_add(self):
        self.conn.add("movies", "rating", "INT")
        self.assertEqual(self.conn.get_columns("movies"), ("title", "year", "watched", "rowid", "rating"), "Failed to add new column")

    def test_getcolumns(self):
        self.assertEqual(self.conn.get_columns("movies"), ("title", "year", "watched", "rowid"), "failed to get column names")

    def test_orderby_asc(self):
        self.assertEqual(self.conn.select("movies", "title", orderby="title, year").fetchone()["title"], "2012", "failed to order ascending")

    def test_orderby_desc(self):
        self.assertEqual(self.conn.select("movies", "title", orderby="title", ordering="DESC").fetchone()["title"], "The Matrix", "failed to order descending")

    def tearDown(self):
        self.conn.drop()  # test to drop database file

if __name__ == "__main__":
    unittest.main()
