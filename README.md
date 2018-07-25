# sqlite-tamer
Module for simple data persistence using the SQLite architecture

The Tamer() class provides easy, painless access to the underlying SQLite database through its methods.

Because it's a subclass of sqlite3.Connection, it inherits all of its methods, so the instance is capable to execute just about everything a normal SQLite connection can do.

Queries altering the database are immediately commited.
