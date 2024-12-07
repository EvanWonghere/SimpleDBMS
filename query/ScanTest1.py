# -*- coding: utf-8 -*-
# @Time    : 2024/12/6 18:34
# @Author  : EvanWong
# @File    : ScanTest1.py
# @Project : TestDB


from simpledb.SimpleDB import SimpleDB
from record.Schema import Schema
from record.Layout import Layout
from record.TableScan import TableScan
from query.Constant import Constant
from query.Term import Term
from query.Expression import Expression
from query.SelectScan import SelectScan
from query.Predicate import Predicate
from query.ProjectScan import ProjectScan
import random

db = SimpleDB("scantest1")
tx = db.newTx()

sch1 = Schema()
sch1.add_int_field("A")
sch1.add_string_field("B", 9)
layout = Layout(sch1)
s1 = TableScan(tx, "T", layout)

s1.before_first()
n = 100
print("Inserting " + str(n) + " random records.")
for i in range(n):
    s1.insert()
    k = random.randint(0, 50)
    print("Inserted " + str(k) + " to records.")
    s1.set_int("A", k)
    s1.set_string("B", "rec" + str(k))
s1.close()

s2 = TableScan(tx, "T", layout)
# 查找所有A=10的记录
c = Constant(10)
t = Term(Expression(field_name="A"), Expression(c))
pred = Predicate(t)
print("The predicate is ", pred)
s3 = SelectScan(s2, pred)
fields = ["B"]
s4 = ProjectScan(s3, fields)
print("获取B")
# print("s4 while start")
while s4.next():
    # print("In s4 while")
    print(s4.get_string("B"))
s4.close()
tx.commit()
