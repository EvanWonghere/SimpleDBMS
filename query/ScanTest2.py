# -*- coding: utf-8 -*-
# @Time    : 2024/12/7 15:27
# @Author  : EvanWong
# @File    : ScanTest2.py
# @Project : TestDB
from simpledb.SimpleDB import SimpleDB
from record.Schema import Schema
from record.Layout import Layout
from record.TableScan import TableScan
from query.Term import Term
from query.Expression import Expression
from query.SelectScan import SelectScan
from query.Predicate import Predicate
from query.ProjectScan import ProjectScan
from query.ProductScan import ProductScan


db = SimpleDB("scantest2")
tx = db.new_tx

sch1 = Schema()
sch1.add_int_field("A")
sch1.add_string_field("B", 9)
layout1 = Layout(sch1)
us1 = TableScan(tx, "T1", layout1)
us1.before_first()
n = 5
print("Inserting " + str(n) + " records into T1.")
for i in range(n):
    us1.insert()
    print(f"Inserted A: {i}")
    us1.set_int("A", i)
    us1.set_string("B", "bbb" + str(i))
us1.close()

sch2 = Schema()
sch2.add_int_field("C")
sch2.add_string_field("D", 9)
layout2 = Layout(sch2)
us2 = TableScan(tx, "T2", layout2)
us2.before_first()
print("Inserting " + str(n) + " records into T2.")
for i in range(n):
    us2.insert()
    print(f"Inserted C: {n - i - 1}")
    us2.set_int("C", n - i - 1)
    us2.set_string("D", "ddd" + str(n - i - 1))
us2.close()

s1 = TableScan(tx, "T1", layout1)
s2 = TableScan(tx, "T2", layout2)
s3 = ProductScan(s1, s2)
# 选择所有A=C的记录
t = Term(Expression(field_name="A"), Expression(field_name="C"))
pred = Predicate(t)
print("The predicate is ", pred)
s4 = SelectScan(s3, pred)

# projecting on [B,D]
c = ["B", "D"]
s5 = ProjectScan(s4, c)
while s5.next():
    print(s5.get_string("B") + " " + s5.get_string("D"))
s5.close()
tx.commit()
