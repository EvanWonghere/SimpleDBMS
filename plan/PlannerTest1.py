# -*- coding: utf-8 -*-
# @Time    : 2024/12/9 15:13
# @Author  : EvanWong
# @File    : PlannerTest1.py
# @Project : TestDB
import random

from simpledb.SimpleDB import SimpleDB

db = SimpleDB("planner_test")
tx = db.new_tx
planner = db.planner
cmd = "create table T1(A int, B varchar(9))"
planner.execute_update(cmd, tx)
n = 5
print("Inserting", n, "random records")
a = 20
b = "rec20"
for i in range(n):
    cmd = "insert into T1(A,B) values(" + str(a) + ", '" + b + "')"
    planner.execute_update(cmd, tx)
    a = random.randint(0, 50)
    b = "rec" + str(a)
print("插入结束")
qry = "select A, B from T1 where A=20"
# :p ProjectPlan
p = planner.create_query_plan(qry, tx)
s = p.open()
while s.next():
    print(s.get_int("A"), s.get_string("B"))
s.close()
tx.commit()
