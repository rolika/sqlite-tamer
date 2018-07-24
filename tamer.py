"""
Module for simple data persistence using the SQLite architecture
"""


import sqlite3
import sys
import os


class Tamer(sqlite3.Connection):
    """Instanciated as a subclass of SQLite-connection object."""
    def __init__(self, db_name=":memory:"):
        """Initialize SQLite3 db-connection.
        If the database file doesn't exist, it'll be created.
        Makes use of Row-object to access through column-names (and indexing).
        For a temporary database in memory, use the default ':memory:' name.

        Args:
            db_name: string containing a database-name (could also be a path-like object)

        Reading:
            https://docs.python.org/3/library/sqlite3.html#connection-objects
        """
        try:
            super().__init__(db_name)
            self.row_factory = sqlite3.Row
        except sqlite3.Error as err:
            sys.exit("Couldn't connect to database: {}".format(err))


    def create(self, table_name, **cols):
        """Create table with provided columns and constraints.
        The table will be created only if it doesn't exist already.
        Immediatley commits after succesful execution of statement.

        Args:
            table_name: string containing a valid table-name
            **cols:     columnname=constraints pairs. An empty string means no constraint.

        Returns:
            boolean:    indicates success

        Reading:
            https://sqlite.org/lang_createtable.html
            https://docs.python.org/3/tutorial/controlflow.html#arbitrary-argument-lists
        """
        try:
            with self:
                self.execute("""CREATE TABLE IF NOT EXISTS {}({})"""\
                             .format(table_name, ", ".join("{} {}".format(colname, constraint)\
                             for colname, constraint in cols.items())))
            return True
        except sqlite3.Error as err:
            print("Couldn't create table:", err, file=sys.stderr)
            return False


    def insert(self, table, **what):
        """Insert new row into database.
        Commits after succesful execution.

        Args:
            table:  string containing a valid table-name
            **what: columnname=value separated by commas.

        Returns:
            primary key of last inserted row or None if insertion failed

        Reading:
            https://sqlite.org/lang_insert.html
            https://docs.python.org/3/library/stdtypes.html#mapping-types-dict
        """
        lastrowid = None
        cols = ", ".join(what.keys())
        qmarks = ", ".join("?" for _ in what)
        values = tuple(what.values())

        try:
            with self:
                lastrowid = self.execute("""INSERT INTO {}({}) VALUES({})"""\
                .format(table, cols, qmarks), values).lastrowid
        except sqlite3.Error as err:
            print("Couldn't insert item:", err, file=sys.stderr)
        return lastrowid


    def select(self, table, logic="OR", *what, **where):
        """Select row(s) from database.
        Using only the mandatory arguments selects everything.

        Args:
            table:      string containing a valid table-name
            logic:      logical operator in the WHERE clause. This simple function won't allow to
                        mix logical operators. Provided without kwargs won't have any effect.
            *what:      list of strings containing columnnames to select
            **where:    narrow selection with column=value pair(s). If more pairs are specified,
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
        
        if what:
            select_stmnt = """SELECT {}""".format(", ".join(col for col in what))
        else:
            select_stmnt = """SELECT *"""
        select_stmnt += """ FROM {}"""

        try:
            if where:
                select_stmnt += self._stmnt("WHERE", logic, **where)
                return self.execute(select_stmnt.format(table), tuple(where.values()))
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
        delete_stmnt = """DELETE FROM {}""" + self._stmnt("WHERE", logic, **kwargs)

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

        update_stmnt = """ UPDATE {}""" + self._stmnt("SET", ",", **what)\
                                        + self._stmnt("WHERE", logic, **where)

        try:
            with self:
                self.execute(update_stmnt.format(table), list(what.values()) + list(where.values()))
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


    def fill(csv_filename, tablename=None, overwrite=False, **columns):
        """Import records from a csv-file
        Default behavior: create a new table in the database with the name of the csv-file (without
        .csv). If the csv comes with header, use the fieldnames as column-names, if not, use
        filename_i, where i is index starting from 0. The csv-rows gets inserted into the table.

        Args:
            csv_filename:   string containing a valid csv-file
            tablename:      string containing a table name. If existing, proceed as in the overwrite
                            argument described.
            overwrite:      False: use the provided existing table (appending rows)
                            True: drop the provided existing table and create a new one
            **columns:      if provided, use these column-names
        
        Returns:
            primary key of last inserted row or None if operation failed
        
        Reading:
            https://docs.python.org/3/library/csv.html
            https://www.python.org/dev/peps/pep-0305/
            https://github.com/rufuspollock/csv2sqlite (although i want something far more simpler)
        """
        pass


    def rename(self, table, new):
        """ Rename table in the database.
        
        Args:
            table:  string containing table to rename
            new:    string containing new name

        Returns:
            boolean:    indicates success
            
        Reading:
            https://sqlite.org/lang_altertable.html
        """
        try:
            with self:
                self.execute("ALTER TABLE {} RENAME TO {}".format(table, new))
            return True
        except sqlite3.Error as err:
            print("Couldn't rename table:", err, file=sys.stderr)
            return False
    
    def add(self, table, column, constraint=""):
        """ Add a new column to table
        
        Args:
            table:      string containing table to alter
            column:     string containing columnname
            constraint: string containing a column constraint

        Returns:
            boolean:    indicates success
            
        Reading:
            https://sqlite.org/lang_altertable.html
        """
        try:
            with self:
                self.execute("ALTER TABLE {} ADD COLUMN {} {}".format(table, column, constraint))
            return True
        except sqlite3.Error as err:
            print("Couldn't add new column:", err, file=sys.stderr)
            return False
    
    def get_columns(self, table):
        """Return all user defined column names in table
        
        Args:
            table:  string containing tablename
        
        Returns:
            tuple of string containing user defined column names or None if an error occured
        
        Reading:
            https://sqlite.org/pragma.html#pragma_table_info
        """
        try:
            with self:
                cols = self.execute("PRAGMA table_info({})".format(table))
            return tuple(col["name"] for col in cols)
        except sqlite3.Error as err:
            print("Couldn't retrieve column names:", err, file=sys.stderr)
            return None

    @staticmethod
    def _stmnt(statement, logic, **kwargs):
        """SQL statement extension"""
        return " {} ".format(statement) + " {} ".format(logic).join("{}{} = ?"\
               .format("NOT " if logic == "NOT" else "", key) for key in kwargs.keys())

