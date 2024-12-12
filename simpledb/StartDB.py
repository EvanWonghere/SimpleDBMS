# -*- coding: utf-8 -*-
# @Time    : 2024/12/9 21:28
# @Author  : EvanWong
# @File    : StartDB.py
# @Project : TestDB
from sqlite3 import Error

from jdbc.embedded.EmbeddedDriver import EmbeddedDriver
from jdbc.embedded.EmbeddedMetadata import EmbeddedMetadata
from jdbc.embedded.EmbeddedResultSet import EmbeddedResultSet
from jdbc.embedded.EmbeddedStatement import EmbeddedStatement
from record.FieldType import FieldType


def doQuery(stmt: EmbeddedStatement, cmd):
    try:
        rs: EmbeddedResultSet = stmt.execute_query(cmd)
        md: EmbeddedMetadata = rs.get_metadata()
        numcols = md.get_column_count()
        totalwidth = 0

        for i in range(1, numcols + 1):
            fldname = md.get_column_name(i)
            width = md.get_column_display_size(i)
            totalwidth += width
            fmt = "%" + str(width) + "s"
            print(fmt % fldname, end="")
        print()
        for i in range(totalwidth):
            print("-", end="")
        print()

        while rs.next():
            for i in range(1, numcols + 1):
                fldname = md.get_column_name(i)
                fldtype = md.get_column_type(i)
                fmt = "%" + str(md.get_column_display_size(i))
                if fldtype == FieldType.INT:
                    ival = rs.get_int(fldname)
                    print((fmt + "d") % ival, end="")
                elif fldtype == FieldType.FLOAT:
                    fval = rs.get_float(fldname)
                    print((fmt + "f") % fval, end="")
                else:
                    sval = rs.get_string(fldname)
                    print((fmt + "s") % sval, end="")
            print()
        for i in range(totalwidth):
            print("-", end="")
        print()
        # else:
        #     tableList = stmt.execute_query(cmd)
        #     tableList.remove("tblcat")
        #     tableList.remove("fldcat")
        #     tableList.remove("viewcat")
        #     print("Tables in " + dbname)
        #     print("----------")
        #     for i in tableList:
        #         print(i)
        #     print("----------")
    except Error as e:
        print("SQL Exception:", e)


def doUpdate(stmt: EmbeddedStatement, cmd: str):
    try:
        print("start update")
        howmany = stmt.execute_update(cmd)
        print(howmany, "records processed")
    except Error as e:
        print("SQL Exception:", e)


def printHelp():
    f = open("help.txt", 'r', encoding='utf-8')
    print(f.read())


if __name__ == "__main__":
    dbname = "test"
    driver = EmbeddedDriver()
    try:
        conn = driver.connect(dbname)
        stmt = conn.create_statement()
        print("输入 -help- 查看帮助")
        while True:
            cmd = input("SQL> ")
            if cmd.startswith("exit"):
                break
            elif cmd == "-help-":
                printHelp()
            elif cmd.startswith("select"):
                doQuery(stmt, cmd)
            else:
                doUpdate(stmt, cmd)
    except Error as e:
        print("SQL Exception:", e)
