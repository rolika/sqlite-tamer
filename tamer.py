"""
Module for simple data persistence using the SQLite architecture
"""


import sqlite3
import sys
import os


class Tamer(sqlite3.Connection):
    """Instanciated as a subclass of SQLite-connection object"""
    def __init__(self, db_name=":memory:"):
        """Initialize SQLite3 db-connection.
        If the database file doesn't exist, it'll be created.
        Makes use of Row-object to access through column-names (and indexing).
        For a temporary database in memory, use the default ':memory:' name.

        Args:
            db_name: string containing a database-name (could also be a path-like object)

        Returns:
            instance of sqlite3.Connection to access the database if succeed, otherwise None

        Reading:
            https://docs.python.org/3/library/sqlite3.html#connection-objects
        """
        super().__init__(db_name)
        self.row_factory = sqlite3.Row

    def create(self, table_name, *column_names):
        """Create table with provided columns.
        The table will be created only if it doesn't exist already.
        Immediatley commits after succesful execution of statement.

        Args:
            table_name:     string containing a valid table-name
            *column_names:  strings separated by commas (arbitrary argument list)
                            Addtionally created columns:
                                1) rowid:       primary key (implicitly created by SQLite3
                                2) added:       date of insertion
                                3) modified:    date of last modification

        Returns:
            boolean:    indicates success

        Reading:
            https://sqlite.org/lang_createtable.html
            https://docs.python.org/3/tutorial/controlflow.html#arbitrary-argument-lists
        """
        try:
            with self:
                self.execute("""CREATE TABLE IF NOT EXISTS {}({}, added, modified)"""\
                             .format(table_name, ", ".join(column_names)))
            return True
        except sqlite3.Error as err:
            print("Couldn't create table:", err, file=sys.stderr)
            return False


    def insert(self, table, **kwargs):
        """Insert new row into database.
        Commits after succesful execution.

        Args:
            table:      string containing a valid table-name
            **kwargs:   columnname=value separated by commas. Unintentionally provided values for
                        'rowid', 'added' and 'modified' will be discarded.

        Returns:
            primary key of last inserted row or None if insertion failed

        Reading:
            https://sqlite.org/lang_insert.html
            https://docs.python.org/3/library/stdtypes.html#mapping-types-dict
        """
        if kwargs.get("rowid"):
            kwargs.pop("rowid")  # discard provided value for rowid as it's system property
        lastrowid = None
        cols = ", ".join(kwargs.keys())
        qmarks = ", ".join("?" for _ in kwargs)
        values = tuple(kwargs.values())
        try:
            with self:
                lastrowid = self.execute("""
                INSERT INTO {}({}, added, modified) VALUES({}, CURRENT_DATE, CURRENT_DATE)"""\
                .format(table, cols, qmarks), values).lastrowid
        except sqlite3.Error as err:
            print("Couldn't insert item:", err, file=sys.stderr)
        return lastrowid


    def select(self, table, logic="OR", **kwargs):
        """Select entire row(s) from database.
        Using only the mandatory arguments selects everything.
        Always selects primary key ('rowid') too.

        Args:
            table:      string containing a valid table-name
            logic:      logical operator in the WHERE clause. This simple function won't allow to
                        mix logical operators. Provided without kwargs won't have any effect.
            **kwargs:   narrow selection with column=value pair(s). If more pairs are specified,
                        they're bound together with the provided logical operator in the WHERE
                        clause of the query. The default 'OR' means any, 'AND' means all of the
                        conditions in kwargs must be met. 'NOT' is only partially supported, it
                        makes only sense with one kwarg, but the latter won't be verified.

        Returns:
            Cursor-object of resulting query (powered as row_factory) or
            None (slite3.Error happened)

        Reading:
            https://sqlite.org/lang_select.html
            https://www.w3schools.com/sql/sql_and_or.asp
        """
        select_stmnt = """SELECT DISTINCT rowid, * FROM {}"""
        try:
            if kwargs:
                select_stmnt += _stmnt("WHERE", logic, **kwargs)
                return self.execute(select_stmnt.format(table), tuple(kwargs.values()))
            return self.execute(select_stmnt.format(table))
        except sqlite3.Error as err:
            print("Couldn't select any item:", err, file=sys.stderr)
            return None


    def delete(self, table, logic="OR", **kwargs):
        """Delete row(s) from database.
        Commits after succesful execution.

        Args:
            table:      string containing a valid table-name
            logic:      logical operator in the WHERE clause. This simple function won't allow to
                        mix logical operators. Provided without kwargs has no sense.
            **kwargs:   specify criteria with column=value pair(s). If more pairs are specified,
                        they're bound together with the provided logical operator in the WHERE
                        clause of the query. The default 'OR' means any, 'AND' means all of the
                        conditions in kwargs must be met. 'NOT' is only partially supported, it
                        makes only sense with one kwarg, but the latter won't be verified.

        Returns:
            boolean:    indicates success

        Reading:
            https://sqlite.org/lang_delete.html
        """
        delete_stmnt = """DELETE FROM {}""" + _stmnt("WHERE", logic, **kwargs)
        try:
            with self:
                self.execute(delete_stmnt.format(table), tuple(kwargs.values()))
            return True
        except sqlite3.Error as err:
            print("Couldn't delete item:", err, file=sys.stderr)
            return False


    def update(self, table, what, logic="OR", **where):
        """Update row(s).
        Commits after succesful execution.

        Args:
            table:      string containing a valid table-name
            what:       dictionary containing column=new_value pairs
            logic:      logical operator in the WHERE clause. This simple function won't allow to
                        mix logical operators.
            **where:    update criteria as column=value pair(s). If more pairs are specified,
                        they're bound together with the provided logical operator in the WHERE
                        clause of the query. The default 'OR' means any, 'AND' means all of the
                        conditions in kwargs must be met. 'NOT' is only partially supported, it
                        makes only sense with one kwarg, but the latter won't be verified.
                         Unintentionally provided values for 'rowid', 'added' and 'modified' will
                         be discarded.

        Returns:
            boolean:    indicates success

        Reading:
            https://sqlite.org/lang_update.html
        """
        # discard provided values for these columns
        if what.get("rowid"):
            what.pop("rowid")
        if what.get("added"):
            what.pop("added")
        if what.get("modified"):
            what.pop("modified")

        update_stmnt = """ UPDATE {}""" + _stmnt("SET", ",", **what)
        update_stmnt += """, modified = CURRENT_DATE""" + _stmnt("WHERE", logic, **where)

        try:
            with self:
                self.execute(update_stmnt.format(table),
                            tuple(list(what.values()) + list(where.values())))
            return True
        except sqlite3.Error as err:
            print("Couldn't update item:", err, file=sys.stderr)
            return False


    def destroy(self, db_name):
        """Delete database file.
        May be necessary as SQLite doesn't support 'DROP DATABASE'.

        Args:
            db_name: string containing a database-name (could also be a path-like object)

        Returns:
            boolean:    indicates success

        Reading:
            https://docs.python.org/3/library/os.html#os.unlink
        """
        try:
            self.close()
            os.unlink(db_name)
            return True
        except (OSError, sqlite3.Error) as err:
            print("Couldn't delete database:", err, file=sys.stderr)
            return False


    def drop(self, table_name):
        """Drop (delete) table.

        Args:
            table:      string containing a valid table-name

        Returns:
            boolean:    indicates success

        Reading:
            https://sqlite.org/lang_droptable.html
        """
        try:
            with self:
                self.execute("""DROP TABLE IF EXISTS {}""".format(table_name))
            return True
        except sqlite3.Error as err:
            print("Couldn't drop table:", err, file=sys.stderr)
            return False


def _stmnt(statement, logic, **kwargs):
    return " {} ".format(statement) + " {} ".format(logic).join("{}{} = ?"\
           .format("NOT " if logic == "NOT" else "", key) for key in kwargs.keys())
