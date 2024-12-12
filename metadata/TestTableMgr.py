# -*- coding: utf-8 -*-
# @Time    : 2024/12/2 18:18
# @Author  : EvanWong
# @File    : TestTableMgr.py
# @Project : TestDB
from record.FieldType import FieldType
from simpledb.SimpleDB import SimpleDB
from TableMgr import TableMgr
from record.Schema import Schema


db = SimpleDB("table_mgr_test", 400, 8)
tx = db.new_tx
tm = TableMgr(True, tx)
sch = Schema()
sch.add_int_field("A")
sch.add_string_field("B", 9)
tm.create_table("MyTable", sch, tx)

layout = tm.get_layout("MyTable", tx)
size = layout.slot_size
sch2 = layout.schema
print("MyTable has slot size ", size)
print("Its fields are:")
for field_name in sch2.fields:
    tp = None
    if sch2.get_field_type(field_name) == FieldType.INT:
        tp = "int"
    else:
        flLen = sch2.get_field_length(field_name)
        tp = "varchar(" + str(flLen) + ")"
    print(field_name, ":", tp)
tx.commit()
