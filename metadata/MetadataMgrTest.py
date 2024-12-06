# -*- coding: utf-8 -*-
# @Time    : 2024/12/3 20:08
# @Author  : EvanWong
# @File    : MetadataMgrTest.py
# @Project : TestDB

from simpledb.SimpleDB import SimpleDB
from MetadataMgr import MetadataMgr
from record.Schema import Schema
from record.TableScan import TableScan


db = SimpleDB("metadatamgrtest", 400, 8)
tx = db.newTx()
mdm = MetadataMgr(True, tx)

sch = Schema()
sch.add_int_field("A")
sch.add_string_field("B", 9)

# print(f"Current fields are: {sch.fields}")

mdm.create_table("MyTable", sch, tx)
layout = mdm.get_layout("MyTable", tx)
print("Generated layout: ", end="")
for name in layout.schema.fields:
    print(f"{name}", end=", ")
size = layout.slot_size
sch2 = layout.schema
print("MyTable has slot size", size)
print("Its fields are:")
for fldname in sch2.fields:
    type_str = "int" if sch2.get_field_type(fldname) == 4 else f"varchar({sch2.get_field_length(fldname)})"
    print(f"{fldname}: {type_str}")

ts = TableScan(tx, "MyTable", layout)
for i in range(50):
    ts.insert()
    n = round(i * 50)
    ts.set_int("A", n)
    ts.set_string("B", f"rec{n}")
si = mdm.get_stat_info("MyTable", layout, tx)
print("B(MyTable) =", si.accessed_blocks)
print("R(MyTable) =", si.output_records)
print("V(MyTable,A) =", si.distinct_values)
print("V(MyTable,B) =", si.distinct_values)

viewdef = "select B from MyTable where A = 1"
mdm.create_view("viewA", viewdef, tx)
v = mdm.get_view_definition("viewA", tx)
print("View def =", v)

mdm.create_index("indexA", "MyTable", "A", tx)
mdm.create_index("indexB", "MyTable", "B", tx)
print("***************************************************************************")
print("Start get info")
idxmap = mdm.get_index_info("MyTable", tx)
print("End get info")
print(f"idxmap is empty : {idxmap == {}}")
print(f"idxmap is {idxmap.keys()}: {idxmap.values()}")
print("***************************************************************************")

ii = idxmap["A"]
print("B(indexA) =", ii.accessed_blocks)
print("R(indexA) =", ii.output_records)
print("V(indexA,A) =", ii.distinct_values("A"))
print("V(indexA,B) =", ii.distinct_values("B"))

ii = idxmap["B"]
print("B(indexB) =", ii.accessed_blocks)
print("R(indexB) =", ii.output_records)
print("V(indexB,A) =", ii.distinct_values("A"))
print("V(indexB,B) =", ii.distinct_values("B"))

tx.commit()

