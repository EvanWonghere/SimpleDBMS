# -*- coding: utf-8 -*-
# @Time    : 2024/12/9 21:28
# @Author  : EvanWong
# @File    : StartDB.py
# @Project : TestDB
import os
from sqlite3 import Error

from jdbc.embedded.EmbeddedDriver import EmbeddedDriver
from jdbc.embedded.EmbeddedMetadata import EmbeddedMetadata
from jdbc.embedded.EmbeddedResultSet import EmbeddedResultSet
from jdbc.embedded.EmbeddedStatement import EmbeddedStatement
from record.FieldType import FieldType


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


def do_update(statement: EmbeddedStatement, command: str):
    try:
        print("start update")
        howmany = statement.execute_update(command)
        print(howmany, "records processed")
    except Error as err:
        print(f"SQL Exception: {err}")


def print_help():
    f = open("help.txt", 'r', encoding='utf-8')
    print(f.read())


def do_show_tables(db_name: str):
    tbl_names: list[str] = []
    max_len = 9
    for table_file_name in os.listdir(db_name):
        if table_file_name.endswith(".tbl") and not table_file_name.endswith("_cat.tbl"):
            tbl_names.append(table_file_name[:-4])
            max_len = max(max_len, len(table_file_name))

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


if __name__ == "__main__":
    dbname = "test"
    driver = EmbeddedDriver()
    try:
        conn = driver.connect(dbname)
        stmt = conn.create_statement()
        print("输入 -help- 查看帮助")
        while True:
            cmd = input("SQL> ").strip().lower()
            if cmd.startswith("exit"):
                break
            elif cmd == "-help-":
                print_help()
            elif cmd.startswith("select"):
                do_query(stmt, cmd)
            elif cmd == "show tables":
                do_show_tables(dbname)
            else:
                do_update(stmt, cmd)
    except Error as e:
        print("SQL Exception:", e)
