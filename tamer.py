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
import json
import pathlib


DEFAULT_FOLDER = "./"
DEFAULT_EXTENSION = "db"


class Tamer(sqlite3.Connection):
    """Instanciated as a subclass of SQLite-connection object."""
    def __init__(self, db_name=":memory:", db_folder:str=DEFAULT_FOLDER, db_ext:str=DEFAULT_EXTENSION, attach:list=[]) -> None:
        """Initialize SQLite3 db-connection.
        If the database file doesn't exist, it'll be created.
        Makes use of Row-object to access through column-names (and indexing).
        For a temporary database in memory, use the default ':memory:' name.

        Args:
            db_name:    string containing a database-name
            db_folder:  string containing the folder in which the database should be created
            db_ext:     string containing the extension for the database name
            attach:     list of databases to attach. Attached name is filename without extension

        Reading:
            https://docs.python.org/3/library/sqlite3.html#connection-objects
        """
        # check database filepath
        pathlib.Path(db_folder).mkdir(exist_ok=True)
        self._db = pathlib.Path(db_folder, f"{db_name}.{db_ext}")
        self._db_folder = db_folder
        self._db_ext = db_ext

        try:
            super().__init__(self._db)
            self.row_factory = sqlite3.Row
            self._attach = {name.split(".")[0]: name for name in attach}
            self.attach(**self._attach)            
        except sqlite3.Error as err:
            sys.exit("Couldn't connect to database: {}".format(err))
    
    def __del__(self):
        """On deletion or garbage collection detach any databases. """
        self.detach(*self._attach)  # the single asteriks * unpacks the keys

    @classmethod
    def create_from_json(cls, jsonfile:str, default:str=None, db_folder:str=DEFAULT_FOLDER, db_ext:str=DEFAULT_EXTENSION) -> dict:
        """Create multiple databases from a json-file.
        If there is an "_attach_" keyword among the column definitions, attach that list of databases.

        Args:
            jsonfile:   string containing filename of a valid json-file which holds the database structure
            default:    json-file containing default column-definitions to be applied to all tables
            db_folder:  string containing the folder in which the database should be created
            db_ext:     string containing the extension for the database name
        
        Returns:
            dict:       dictionary containing database-names=connection pairs
        """
        # read json files
        with open(jsonfile) as f:
            db_struct = json.load(f)
        if default:
            with open(default) as f:
                default = json.load(f)
            for db_name in db_struct:
                for table in db_struct[db_name]:
                    if table != "_attach_":
                        db_struct[db_name][table].update(default)  # apply default columns

        # connect to/create database files
        conns = dict()
        for db_name in db_struct:
            print(f"Connect to database: {db_name}")
            attach = db_struct[db_name].pop("_attach_")
            conns[db_name] = cls(db_name, db_folder, db_ext, attach)
            # create tables if they don't exist already
            for table, cols in db_struct[db_name].items():
                conns[db_name].create(table, **cols)
        
        return conns

    def create(self, table, *cols, **constr) -> bool:
        """Create table with provided columns and/or constraints.
        The table will be created only if it doesn't exist already.

        Args:
            table:      string containing a valid table-name
            *cols:      tuple of column name strings without constraints
            **constr:   columnname=constraints pairs

        Returns:
            boolean:    indicates success

        Reading:
            https://sqlite.org/lang_createtable.html
            https://docs.python.org/3/tutorial/controlflow.html#arbitrary-argument-lists
        """
        cols = {col: "" for col in cols}
        cols.update(constr)
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
                self._db.unlink()
                return True
            except (FileNotFoundError, sqlite3.Error) as err:
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
        It is assumed, that the attachable database files are in the same folder and have the same extensions.

        Args:
            kwargs: schemaname=database name key-value pairs (database name without folder and extension)
        
        Returns:
            boolean value depending on wether the attach was successful or not
        
        Reading:
            https://sqlite.org/lang_attach.html
        """
        try:
            with self:
                for schemaname, db_name in kwargs.items():
                    file = pathlib.Path(self._db_folder, f"{db_name}.{self._db_ext}")
                    self.execute("""ATTACH DATABASE ? AS ?;""", (file, schemaname))
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

