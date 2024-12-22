# -*- coding: utf-8 -*-
# @Time    : 2024/12/9 21:28
# @Author  : EvanWong
# @File    : StartDB.py
# @Project : TestDB
import os
import sys
from sqlite3 import Error

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
    try:
        rs: EmbeddedResultSet = statement.execute_query(command)
        md: EmbeddedMetadata = rs.get_metadata()
        columns = md.get_column_count()
        total_width = 0

        for i in range(1, columns + 1):
            field_name = md.get_column_name(i)
            width = md.get_column_display_size(i)
            total_width += width
            fmt = "%" + str(width) + "s"
            print(fmt % field_name, end="")
        print()
        for i in range(total_width):
            print("-", end="")
        print()

        while rs.next():
            for i in range(1, columns + 1):
                field_name = md.get_column_name(i)
                field_type = md.get_column_type(i)
                fmt = "%" + str(md.get_column_display_size(i))
                if field_type == FieldType.INT:
                    int_val = rs.get_int(field_name)
                    print((fmt + "d") % int_val, end="")
                elif field_type == FieldType.FLOAT:
                    float_val = rs.get_float(field_name)
                    print((fmt + "f") % float_val, end="")
                else:
                    str_val = rs.get_string(field_name)
                    print((fmt + "s") % str_val, end="")
            print()
        for i in range(total_width):
            print("-", end="")
        print()
    except Error as err:
        print("SQL Exception:", err)


def do_update(statement: EmbeddedStatement, command: str, db_name: str):
    try:
        print("start update")
        howmany = statement.execute_update(command)
        print(howmany, "records processed")
    except Error as err:
        print(f"SQL Exception: {err}")


def print_help():
    f = open("help.txt", 'r', encoding='utf-8')
    print(f.read())


def do_show_tables(tx: Transaction):
    tbl_names: list[str] = []
    max_len = 9
    table_cat_schema = Schema()
    table_cat_schema.add_string_field("table_name", TableMgr.MAX_NAME_LENGTH)
    table_cat_schema.add_int_field("slot_size")
    ts = TableScan(tx, "table_cat", Layout(table_cat_schema))
    ts.before_first()
    while ts.next():
        table_name = ts.get_string("table_name")
        if table_name is not None and not table_name.endswith("_cat"):
            tbl_names.append(ts.get_string("table_name"))

    for _ in range(max_len):
        print('-', end="")
    print("")
    for _ in range((max_len - 6) // 2):
        print(" ", end="")
    print("tables")
    for _ in range(max_len):
        print('-', end="")
    print("")
    for table_name in tbl_names:
        white_space_length = (max_len - len(table_name)) // 2
        for _ in range(white_space_length):
            print(" ", end="")
        print(table_name)
    for _ in range(max_len):
        print('-', end="")
    print("")

def do_show_dbs():
    db_names: list[str] = []
    max_len = 13
    for path in os.listdir():
        if os.path.isdir(path) and path.endswith(".db"):
            db_names.append(path[:-3])

    for _ in range(max_len):
        print('-', end="")
    print("")
    for _ in range((max_len - 10) // 2):
        print(" ", end="")
    print("databases")
    for _ in range(max_len):
        print('-', end="")
    print("")
    for db_name in db_names:
        white_space_length = (max_len - len(db_name)) // 2
        for _ in range(white_space_length):
            print(" ", end="")
        print(db_name)
    for _ in range(max_len):
        print('-', end="")
    print("")

def show_notification():
    print("Please to choose a database first.")
    print("Type `create {database_name}` to create a new database,}`")
    print("or type `use {database_name}` to use the existing database.}`")


if __name__ == "__main__":
    dbname = ""
    driver = EmbeddedDriver()
    conn = None
    stmt = None
    where = "SQL"
    try:
        print("Type `help` to see available commands")
        while True:
            cmd = input(where + "> ").strip()
            if cmd.startswith("exit"):
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
                dbname = cmd.split(" ")[-1] + ".db"
                if os.path.exists(dbname):
                    print(f"Database {dbname[:-3]} already exists, type `use {dbname[:-3]}` to use the database.")
                else:
                    conn = driver.connect(dbname)
                    stmt = conn.create_statement()
                    print(f"Database created. Changed to use {dbname[:-3]}.")
                    where = f"SQL/{dbname[:-3]}"
            elif cmd.lower().split(" ")[0] == "use":
                dbname = cmd.split(" ")[-1] + ".db"
                if not os.path.exists(dbname):
                    print(f"Database {dbname[:-3]} does not exist, type `create {dbname[:-3]}` to create the database.")
                else:
                    print(f"Using database {dbname[:-3]}")
                    where = f"SQL/{dbname[:-3]}"
                    conn = driver.connect(dbname)
                    stmt = conn.create_statement()
            else:
                if stmt is None:
                    show_notification()
                else:
                    do_update(stmt, cmd, dbname)
    except Error as e:
        print("SQL Exception:", e)
