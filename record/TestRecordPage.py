# -*- coding: utf-8 -*-
# @Time    : 2024/12/2 17:21
# @Author  : EvanWong
# @File    : TestRecordPage.py
# @Project : TestDB

import random
from simpledb.SimpleDB import SimpleDB
from Layout import Layout
from Schema import Schema
from RecordPage import RecordPage


db = SimpleDB("recordtest", 400, 8)
tx = db.newTx()
sch = Schema()
sch.add_int_field("A")
sch.add_string_field("B", 9)
layout = Layout(sch)
for fldname in layout.schema.fields:
    offset = layout.get_offset(fldname)
    print(fldname, "has offset ", offset)
blk = tx.append("testfile")
tx.pin(blk)
rp = RecordPage(tx, blk, layout)
rp.format()
print("Filling the page with random records.")
slot = rp.insert_after(-1)
while slot >= 0:
    n = random.randint(0, 50)
    rp.set_int(slot, "A", n)
    rp.set_string(slot, "B", "rec" + str(n))
    print("inserting into slot", slot, ": {", n, ",", "rec" + str(n) + " }")
    slot = rp.insert_after(slot)
print("Deleting these records, whose A-values are less than 25.")
count = 0
slot = rp.next_after(-1)
while slot >= 0:
    a = rp.get_int(slot, "A")
    b = rp.get_string(slot, "B")
    if a < 25:
        count += 1
        print("slot ", slot, ": {", a, ", " + b + " }")
        rp.delete(slot)
    slot = rp.next_after(slot)
print(count, "values under 25 were deleted.")
print("Here are the remaining records.")
slot = rp.next_after(-1)
while slot >= 0:
    a = rp.get_int(slot, "A")
    b = rp.get_string(slot, "B")
    print("slot", slot, ": {", a, ", " + b + " }")
    slot = rp.next_after(slot)
tx.unpin(blk)
tx.commit()

