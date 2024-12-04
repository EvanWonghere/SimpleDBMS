# -*- coding: utf-8 -*-
# @Time    : 2024/12/2 18:18
# @Author  : EvanWong
# @File    : TestTableMgr.py
# @Project : TestDB
from record.FieldType import FieldType
from simpledb.SimpleDB import SimpleDB
from TableMgr import TableMgr
from record.Schema import Schema


db = SimpleDB("tblmgrtest", 400, 8)
tx = db.newTx()
tm = TableMgr(True, tx)
sch = Schema()
sch.add_int_field("A")
sch.add_string_field("B", 9)
tm.create_table("Mytable", sch, tx)

layout = tm.get_layout("Mytable", tx)
size = layout.slot_size
sch2 = layout.schema
print("MyTable has slot size ", size)
print("Its fields are:")
for fldname in sch2.fields:
    Thetype = None
    if sch2.get_field_type(fldname) == FieldType.INT:
        Thetype = "int"
    else:
        flLen = sch2.get_field_length(fldname)
        Thetype = "varchar(" + str(flLen) + ")"
    print(fldname, ":", Thetype)
tx.commit()
