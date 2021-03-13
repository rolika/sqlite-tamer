"""
Module for simple data persistence using the SQLite architecture

MIT License

Copyright (c) 2018 Weisz Roland

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
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
        self._db_name = db_name


    def create(self, table, **cols):
        """Create table with provided columns and constraints.
        The table will be created only if it doesn't exist already.

        Args:
            table:  string containing a valid table-name
            **cols: columnname=constraints pairs. An empty string means no constraint.

        Returns:
            boolean:    indicates success

        Reading:
            https://sqlite.org/lang_createtable.html
            https://docs.python.org/3/tutorial/controlflow.html#arbitrary-argument-lists
        """
        try:
            with self:
                self.execute("""CREATE TABLE IF NOT EXISTS {}({})"""\
                             .format(table, ", ".join("{} {}".format(colname, constraint)\
                             for colname, constraint in cols.items())))
            return True
        except sqlite3.Error as err:
            print("Couldn't create table:", err, file=sys.stderr)
            return False


    def insert(self, table, **kwargs):
        """Insert new row into database.
        Commits after succesful execution.

        Args:
            table:      string containing a valid table-name
            **kwargs:   columnname=value separated by commas.

        Returns:
            primary key of last inserted row or None if insertion failed

        Reading:
            https://sqlite.org/lang_insert.html
            https://docs.python.org/3/library/stdtypes.html#mapping-types-dict
        """
        lastrowid = None
        cols = ", ".join(kwargs.keys())
        qmarks = ", ".join("?" for _ in kwargs)
        values = tuple(kwargs.values())

        try:
            with self:
                lastrowid = self.execute("""INSERT INTO {}({}) VALUES({})"""\
                .format(table, cols, qmarks), values).lastrowid
        except sqlite3.Error as err:
            print("Couldn't insert item:", err, file=sys.stderr)
        return lastrowid


    def select(self, table, *cols, **kwargs):
        """Select row(s) from database.
        Using only the mandatory arguments selects everything.

        Args:
            table:      string containing a valid table-name
            *cols:      list of strings containing columnnames to select
            **kwargs:   narrow selection with column=value pair(s). If more pairs are specified,
                        they're bound together with the provided logical operator in the WHERE
                        clause of the query.
                        Special keys:
                            logic:      defaults to OR. NOT is only partially supported, it
                                        makes only sense with one kwarg, but the latter won't be
                                        verified.
                            orderby:    specify the ORDER BY clause
                            ordering:   defaults to ASC, specify DESC if you want descending order
                            distinct:   select distinct values

        Returns:
            Cursor-object of resulting query (powered as row_factory) or
            None (slite3.Error happened)

        Reading:
            https://sqlite.org/lang_select.html
            https://www.w3schools.com/sql/sql_and_or.asp
        """
        distinct = kwargs.pop("distinct", "")
        if distinct:
            distinct = " DISTINCT"

        if cols:
            select_stmnt = """SELECT{} {}""".format(distinct, ", ".join(col for col in cols))
            print(select_stmnt)
        else:
            select_stmnt = """SELECT{} *""".format(distinct)
        select_stmnt += """ FROM {}"""

        logic = kwargs.pop("logic", "OR")
        orderby = kwargs.pop("orderby", "")
        ordering = kwargs.pop("ordering", "ASC")

        if orderby:
            orderby = " ORDER BY " + orderby + " " + ordering

        try:
            if kwargs:
                select_stmnt += self._stmnt("WHERE", logic, **kwargs)
                return self.execute(select_stmnt.format(table) + orderby, tuple(kwargs.values()))
            return self.execute(select_stmnt.format(table) + orderby)
        except sqlite3.Error as err:
            print("Couldn't select any item:", err, file=sys.stderr)
            return None


    def delete(self, table, **kwargs):
        """Delete row(s) from database.

        Args:
            table:      string containing a valid table-name
            **kwargs:   specify criteria with column=value pair(s). If more pairs are specified,
                        they're bound together with the provided logical operator in the WHERE
                        clause of the query.
                        Special keys:
                            logic:      defaults to OR. NOT is only partially supported, it
                                        makes only sense with one kwarg, but the latter won't be
                                        verified.

        Returns:
            boolean:    indicates success

        Reading:
            https://sqlite.org/lang_delete.html
        """
        logic = kwargs.pop("logic", "OR")
        delete_stmnt = """DELETE FROM {}""" + self._stmnt("WHERE", logic, **kwargs)

        try:
            with self:
                self.execute(delete_stmnt.format(table), tuple(kwargs.values()))
            return True
        except sqlite3.Error as err:
            print("Couldn't delete item:", err, file=sys.stderr)
            return False


    def update(self, table, what, **where):
        """Update values in existing row(s) in the database.

        Args:
            table:      string containing a valid table-name
            what:       dictionary containing column=new_value pairs
            **where:    update criteria as column=value pair(s). If more pairs are specified,
                        they're bound together with the provided logical operator in the WHERE
                        clause of the query.
                        Special keys:
                            logic:      defaults to OR. NOT is only partially supported, it
                                        makes only sense with one kwarg, but the latter won't be
                                        verified.

        Returns:
            boolean:    indicates success

        Reading:
            https://sqlite.org/lang_update.html
        """
        logic = where.pop("logic", "OR")
        update_stmnt = """ UPDATE {}""" + self._stmnt("SET", ",", **what)\
                                        + self._stmnt("WHERE", logic, **where)

        try:
            with self:
                self.execute(update_stmnt.format(table), list(what.values()) + list(where.values()))
            return True
        except sqlite3.Error as err:
            print("Couldn't update item:", err, file=sys.stderr)
            return False


    def drop(self, table=None, column=None):
        """Drop (delete) database, table or column.
        If table and column omitted, the method deletes the database-file. With table provided, it
        drops the table. With table and column provided, that column will be deleted.
        If a column should be dropped, the method only verifies that no foreign key references are
        violated. Indexes, triggers, views must be recreated by the user.

        Args:
            table:  string containing a valid table-name
            column: string containing a valid column-name

        Returns:
            boolean:    indicates success

        Reading:
            https://sqlite.org/lang_altertable.html
            https://sqlite.org/lang_droptable.html
            https://www.sqlite.org/faq.html#q11
            https://docs.python.org/3/library/os.html#os.unlink
        """
        if table and column:
            if  column not in self.get_columns(table):
                print("'{}' doesn't exist in '{}'".format(column, table), file=sys.stderr)
                return False
            try:
                with self:
                    fkeys = "ON" if self.execute("""PRAGMA foreign_keys""").fetchone()[0] else "OFF"
                    newcols = list(self.get_columns(table))
                    newcols.remove(column)
                    newcols = ", ".join(newcols)
                    self.executescript("""  PRAGMA foreign_keys=OFF;
                                            CREATE TEMPORARY TABLE tamer_tmp ({newcols});
                                            INSERT INTO tamer_tmp SELECT {newcols} FROM {table};
                                            DROP TABLE {table};
                                            {create_table_again};
                                            INSERT INTO {table} SELECT {newcols} FROM tamer_tmp;
                                            DROP TABLE tamer_tmp;"""\
                                            .format(newcols=newcols, table=table,
                                                    create_table_again=self._sql(table, column)))
                    if len(self.execute("""PRAGMA foreign_key_check""").fetchall()):
                        raise sqlite3.Error("Foreign keys violated!")
                    self.execute("""PRAGMA foreign_keys={}""".format(fkeys))
                return True
            except sqlite3.Error as err:
                print("Couldn't drop column:", err, file=sys.stderr)
                return False

        elif table:
            try:
                with self:
                    self.execute("""DROP TABLE IF EXISTS {}""".format(table))
                return True
            except sqlite3.Error as err:
                print("Couldn't drop table:", err, file=sys.stderr)
                return False

        else:
            try:
                self.close()
                os.unlink(self._db_name)
                return True
            except (OSError, sqlite3.Error) as err:
                print("Couldn't delete database:", err, file=sys.stderr)
                return False


    def rename(self, table, new):
        """Rename existing table in the database.

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
                self.execute("""ALTER TABLE {} RENAME TO {}""".format(table, new))
            return True
        except sqlite3.Error as err:
            print("Couldn't rename table:", err, file=sys.stderr)
            return False


    def add(self, table, column, constraint=""):
        """Add a new column to an existing table.

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
                self.execute("""ALTER TABLE {} ADD COLUMN {} {}"""\
                             .format(table, column, constraint))
            return True
        except sqlite3.Error as err:
            print("Couldn't add new column:", err, file=sys.stderr)
            return False


    def get_columns(self, table):
        """Return all user defined column names in table.

        Args:
            table:  string containing tablename

        Returns:
            tuple of strings containing user defined column names or None if an error occured

        Reading:
            https://sqlite.org/pragma.html#pragma_table_info
        """
        try:
            with self:
                cols = self.execute("""PRAGMA table_info({})""".format(table))
            return tuple(col["name"] for col in cols)
        except sqlite3.Error as err:
            print("Couldn't retrieve column names:", err, file=sys.stderr)
            return None


    def get_tables(self):
        """Return all user defined table names in the database.

        Args:
            none

        Returns:
            tuple of strings containing user defined table names or None if an error occured

        Reading:
            https://stackoverflow.com/questions/82875/
        """
        try:
            with self:
                tables = self.select("sqlite_master", "name", type="table")
            return tuple(table["name"] for table in tables)
        except sqlite3.Error as err:
            print("Couldn't retrieve table names:", err, file=sys.stderr)
            return None
    

    def attach(self, **kwargs):
        """Add another database file to the current database connection.

        Args:
            kwargs: schemaname=filename key-value pairs
        
        Returns:
            boolean value depending on wether the attach was successful or not
        
        Reading:
            https://sqlite.org/lang_attach.html
        """
        try:
            with self:
                for schemaname, filename in kwargs.items():
                    self.execute("""ATTACH DATABASE ? AS ?;""", (filename, schemaname))
            return True        
        except sqlite3.Error as err:
            print("Couldn't attach database:", err, file=sys.stderr)
            return False
    

    def detach(self, *schemanames):
        """Detach an additional database connection previously attached using the ATTACH statement.

        Args:            
            schemanames:    reference name(s) of the database file
        
        Returns:
            boolean value depending on wether the detach was successful or not
        
        Reading:
            https://sqlite.org/lang_detach.html
        """
        try:
            with self:
                for schemaname in schemanames:
                    self.execute("""DETACH DATABASE ?;""", (schemaname, ))
            return True        
        except sqlite3.Error as err:
            print("Couldn't detach database:", err, file=sys.stderr)
            return False


    @staticmethod
    def _stmnt(statement, logic, **kwargs):
        """SQL statement extension"""
        return " {} ".format(statement) + " {} ".format(logic).join("{}{} = ?"\
               .format("NOT " if logic == "NOT" else "", key) for key in kwargs.keys())


    def _sql(self, table, column):
        """Recreate table's create statement without column"""
        sql = self.select("sqlite_master", "sql", type="table").fetchone()["sql"].partition(column)
        comma = sql[2].find(",")  # index of first comma after column to remove
        if comma < 0:  # last column in the create statement
            return sql[0][:sql[0].rfind(",")] + ")"  # need to cut off last comma before column
        return sql[0] + sql[2][comma+1:]  # jump over column in last part

