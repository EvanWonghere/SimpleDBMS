# -*- coding: utf-8 -*-
# @Time    : 2024/12/7 15:47
# @Author  : EvanWong
# @File    : ProductTest.py
# @Project : TestDB
from simpledb.SimpleDB import SimpleDB
from record.Schema import Schema
from record.Layout import Layout
from record.TableScan import TableScan
from query.ProductScan import ProductScan

db = SimpleDB("product_test")
tx = db.new_tx
sch1 = Schema()
sch1.add_int_field("A")
sch1.add_string_field("B", 9)
layout1 = Layout(sch1)
ts1 = TableScan(tx, "T1", layout1)

sch2 = Schema()
sch2.add_int_field("C")
sch2.add_string_field("D", 9)
layout2 = Layout(sch2)
ts2 = TableScan(tx, "T2", layout2)

ts1.before_first()
n = 3
print("Inserting ", n, "record to T1")
for i in range(n):
    ts1.insert()
    ts1.set_int("A", i)
    ts1.set_string("B", "aaa" + str(i))
ts1.close()

ts2.before_first()
print("Inserting ", n, "record to T2")
for i in range(n):
    ts2.insert()
    ts2.set_int("C", n - i - 1)
    ts2.set_string("D", "bbb" + str(n - i - 1))
ts2.close()

s1 = TableScan(tx, "T1", layout1)
s2 = TableScan(tx, "T2", layout2)
s3 = ProductScan(s1, s2)
while s3.next():
    print(s3.get_string("B"))
s3.close()
tx.commit()
