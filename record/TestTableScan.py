# -*- coding: utf-8 -*-
# @Time    : 2024/12/2 17:25
# @Author  : EvanWong
# @File    : TestTableScan.py
# @Project : TestDB


import random
from simpledb.SimpleDB import SimpleDB
from Schema import Schema
from Layout import Layout
from TableScan import TableScan


db = SimpleDB("tabletest", 400, 8)
tx = db.newTx()
sch = Schema()
sch.add_int_field("A")
sch.add_string_field("B", 9)
layout = Layout(sch)
for fldname in layout.schema.fields:
    offset = layout.get_offset(fldname)
    print(fldname, "has offset", offset)
print("Filling the table with 50 random records.")
ts = TableScan(tx, "T", layout)
for i in range(50):
    ts.insert()
    n = random.randint(0, 50)
    ts.set_int("A", n)
    ts.set_string("B", "rec" + str(n))
    print("inserting into slot ", ts.get_rid(), ": {" + str(n) + ", " + "rec" + str(n) + " }")
print("Deleting these records, whose A-values are less than 25.")
count = 0
ts.before_first()
while ts.next():
    a = ts.get_int("A")
    b = ts.get_string("B")
    if a < 25:
        count += 1
        print("slot ", ts.get_rid(), ": {", a, ",", b + "}")
        ts.delete()
print(count, "values under 25 were deleted.")
print("Here are the remaining records.")
ts.before_first()
while ts.next():
    a = ts.get_int("A")
    b = ts.get_string("B")
    print("slot ", ts.get_rid(), ": {", a, ",", b + "}")
ts.close()
tx.commit()
