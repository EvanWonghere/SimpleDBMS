# -*- coding: utf-8 -*-
# @Time    : 2024/12/2 17:14
# @Author  : EvanWong
# @File    : TableMgr.py
# @Project : TestDB
from record.FieldType import FieldType
from record.Layout import Layout
from record.Schema import Schema, FieldInfo
from record.TableScan import TableScan
from tx.Transaction import Transaction


class TableMgr:

    MAX_NAME_LENGTH = 16

    def __init__(self, is_new: bool, tx: Transaction):
        tcatSchema = Schema()
        tcatSchema.add_string_field("table_name", self.MAX_NAME_LENGTH)
        tcatSchema.add_int_field("slot_size")
        self.__tcatLayout = Layout(tcatSchema)

        fcatSchema = Schema()
        fcatSchema.add_string_field("table_name", self.MAX_NAME_LENGTH)
        fcatSchema.add_string_field("field_name", self.MAX_NAME_LENGTH)
        fcatSchema.add_int_field("type")
        fcatSchema.add_int_field("length")
        fcatSchema.add_int_field("offset")
        self.__fcatLayout = Layout(fcatSchema)

        if is_new:
            self.create_table("table_cat", tcatSchema, tx)
            self.create_table("field_cat", fcatSchema, tx)

    def create_table(self, table_name: str, schema: Schema, tx: Transaction):
        layout = Layout(schema)
        tcat = TableScan(tx, "table_cat", self.__tcatLayout)
        tcat.insert()
        tcat.set_string("table_name", table_name)
        tcat.set_int("slot_size", layout.slot_size)

        fcat = TableScan(tx, "field_cat", self.__fcatLayout)
        for field_name in schema.fields:
            fcat.insert()
            fcat.set_string("table_name", table_name)
            fcat.set_string("field_name", field_name)
            fcat.set_int("type", schema.get_field_type(field_name).value)
            fcat.set_int("length", schema.get_field_length(field_name))
            fcat.set_int("offset", layout.get_offset(field_name))
        fcat.close()

    def get_layout(self, table_name: str, tx: Transaction) -> Layout:
        size = -1
        tcat = TableScan(tx, "table_cat", self.__tcatLayout)
        while tcat.next():
            if tcat.get_string("table_name") == table_name:
                size = tcat.get_int("slot_size")
                break
        tcat.close()

        schema = Schema()
        offsets = {}
        fcat = TableScan(tx, "field_cat", self.__fcatLayout)
        while fcat.next():
            if fcat.get_string("table_name") == table_name:
                field_name = fcat.get_string("field_name")
                field_type = fcat.get_int("type")
                field_length = fcat.get_int("length")
                offset = fcat.get_int("offset")
                offsets[field_name] = offset
                schema.add_field(field_name, FieldInfo(FieldType(field_type), field_length))
        fcat.close()

        return Layout(schema, offsets, size)
