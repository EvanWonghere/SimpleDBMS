# -*- coding: utf-8 -*-
# @Time    : 2024/12/9 21:28
# @Author  : EvanWong
# @File    : StartDB.py
# @Project : TestDB

import os
import sys
from sqlite3 import Error

# Adjust path if needed
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from jdbc.embedded.EmbeddedDriver import EmbeddedDriver
from jdbc.embedded.EmbeddedMetadata import EmbeddedMetadata
from jdbc.embedded.EmbeddedResultSet import EmbeddedResultSet
from jdbc.embedded.EmbeddedStatement import EmbeddedStatement
from metadata.TableMgr import TableMgr
from record.FieldType import FieldType
from record.Layout import Layout
from record.Schema import Schema
from record.TableScan import TableScan
from tx.Transaction import Transaction

def do_query(statement: EmbeddedStatement, command: str):
    """
    Execute a SELECT query with the given statement, display the results in a tabular format.

    Args:
        statement (EmbeddedStatement): The statement to use for execution.
        command (str): The SELECT query.

    Raises:
        Error: If SQL exception occurs during query.
    """
    try:
        rs: EmbeddedResultSet = statement.execute_query(command)
        md: EmbeddedMetadata = rs.get_metadata()
        columns = md.get_column_count()
        total_width = 0

        # Print column headers
        col_widths = []
        for i in range(1, columns + 1):
            field_name = md.get_column_name(i)
            width = md.get_column_display_size(i)
            col_widths.append(width)
            total_width += width
            fmt = f"%{width}s"
            print(fmt % field_name, end="")
        print()

        # Print separator line
        print("-" * total_width)

        # Print rows
        while rs.next():
            for i in range(1, columns + 1):
                field_name = md.get_column_name(i)
                field_type = md.get_column_type(i)
                width = col_widths[i - 1]
                if field_type == FieldType.INT:
                    int_val = rs.get_int(field_name)
                    fmt = f"%{width}d"
                    print(fmt % int_val, end="")
                elif field_type == FieldType.FLOAT:
                    float_val = rs.get_float(field_name)
                    fmt = f"%{width}.3f"
                    print(fmt % float_val, end="")
                else:
                    str_val = rs.get_string(field_name)
                    fmt = f"%{width}s"
                    print(fmt % str_val, end="")
            print()

        # Print final separator
        print("-" * total_width)
        rs.close()
    except Error as err:
        print("SQL Exception:", err)

def do_update(statement: EmbeddedStatement, command: str, db_name: str):
    """
    Execute an update command (INSERT, DELETE, UPDATE, CREATE ...).
    Print the number of processed records or 0 if DDL.

    Args:
        statement (EmbeddedStatement): The statement to use for execution.
        command (str): The update command string.
        db_name (str): The current database name (unused here, but provided).
    """
    try:
        print("start update")
        howmany = statement.execute_update(command)
        print(howmany, "records processed")
    except Error as err:
        print(f"SQL Exception: {err}")

def print_help():
    """
    Print usage instructions from help.txt file if it exists, or show minimal instructions otherwise.
    """
    if os.path.exists("help.txt"):
        with open("help.txt", 'r', encoding='utf-8') as f:
            print(f.read())
    else:
        print("""
Commands:
  help                  : Show this help message
  show databases        : List all directories ending in '.db'
  create database <db>  : Create a new database <db>
  use <db>              : Switch to the specified database
  show tables           : Show table list of the current database
  select ...            : Execute a query
  insert/delete/update  : Execute update statement
  exit/quit             : Quit the program
        """)

def do_show_tables(tx: Transaction):
    """
    Print all table names (excluding ..._cat tables) from the database by scanning table_cat.

    Args:
        tx (Transaction): The current transaction.
    """
    tbl_names: list[str] = []
    max_len = 9
    table_cat_schema = Schema()
    table_cat_schema.add_string_field("table_name", TableMgr.MAX_NAME_LENGTH)
    table_cat_schema.add_int_field("slot_size")
    tcat_layout = Layout(table_cat_schema)
    ts = TableScan(tx, "table_cat", tcat_layout)
    ts.before_first()

    while ts.next():
        table_name = ts.get_string("table_name")
        if table_name and not table_name.endswith("_cat"):
            tbl_names.append(table_name)
    ts.close()

    # Print nice table
    print("-" * max_len)
    print("  tables")
    print("-" * max_len)
    for table_name in tbl_names:
        print(table_name.center(max_len))
    print("-" * max_len)

def do_show_dbs():
    """
    Print all directories in the current folder ending in '.db' as databases.
    """
    db_names: list[str] = []
    max_len = 13
    for path in os.listdir():
        if os.path.isdir(path) and path.endswith(".db"):
            db_names.append(path[:-3])

    print("-" * max_len)
    print(" databases".center(max_len))
    print("-" * max_len)
    for db_name in db_names:
        print(db_name.center(max_len))
    print("-" * max_len)

def show_notification():
    """
    If user tries to run a query or update without a selected database, show instructions.
    """
    print("Please choose a database first.")
    print("Type `create database <name>` to create a new database,")
    print("or type `use <name>` to use an existing database.")

if __name__ == "__main__":
    dbname = ""
    driver = EmbeddedDriver()
    conn = None
    stmt = None
    prompt = "SQL"

    try:
        print("Type `help` to see available commands.")
        while True:
            cmd = input(f"{prompt}> ").strip()
            if cmd.lower() in ("exit", "quit"):
                break
            elif cmd == "help":
                print_help()
            elif cmd.lower().startswith("select"):
                if stmt is None:
                    show_notification()
                else:
                    do_query(stmt, cmd)
            elif cmd.lower() == "show tables":
                if stmt is None:
                    show_notification()
                else:
                    do_show_tables(conn.get_transaction())
            elif cmd.lower() == "show databases":
                do_show_dbs()
            elif cmd.lower().startswith("create database"):
                # create database <name>
                parts = cmd.split()
                if len(parts) < 3:
                    print("Usage: create database <dbname>")
                    continue
                dbname = parts[-1] + ".db"
                if os.path.exists(dbname):
                    print(f"Database {dbname[:-3]} already exists. Type `use {dbname[:-3]}`.")
                else:
                    conn = driver.connect(dbname)
                    stmt = conn.create_statement()
                    print(f"Database created. Changed to use {dbname[:-3]}.")
                    prompt = f"SQL/{dbname[:-3]}"
            elif cmd.lower().split()[0] == "use":
                # use <dbname>
                parts = cmd.split()
                if len(parts) < 2:
                    print("Usage: use <dbname>")
                    continue
                dbname = parts[-1] + ".db"
                if not os.path.exists(dbname):
                    print(f"Database {dbname[:-3]} does not exist. Type `create database {dbname[:-3]}` to create.")
                else:
                    print(f"Using database {dbname[:-3]}.")
                    prompt = f"SQL/{dbname[:-3]}"
                    conn = driver.connect(dbname)
                    stmt = conn.create_statement()
            elif cmd:
                # Non-empty, treat as update
                if stmt is None:
                    show_notification()
                else:
                    do_update(stmt, cmd, dbname)
    except Error as e:
        print("SQL Exception:", e)