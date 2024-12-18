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

# TODO: Add feature that be able to add column to the existed table.

class TableMgr:
    """Table Manager
    To create table and store its metadata to directory, and read the metadata of the former created table.

    Attributes:
        MAX_NAME_LENGTH (int): The maximum length of the table name.

        __table_cat_layout (Layout): The layout of the table, to record the table name and slot size.
        __field_cat_layout (Layout): The layout of the field, to record the field infos of each field.

    """

    MAX_NAME_LENGTH = 16

    def __init__(self, is_new: bool, tx: Transaction):
        table_cat_schema = Schema()
        table_cat_schema.add_string_field("table_name", self.MAX_NAME_LENGTH)
        table_cat_schema.add_int_field("slot_size")
        self.__table_cat_layout: Layout = Layout(table_cat_schema)

        field_cat_schema = Schema()
        field_cat_schema.add_string_field("table_name", self.MAX_NAME_LENGTH)
        field_cat_schema.add_string_field("field_name", self.MAX_NAME_LENGTH)
        field_cat_schema.add_int_field("type")
        field_cat_schema.add_int_field("length")
        field_cat_schema.add_int_field("offset")
        self.__field_cat_layout: Layout = Layout(field_cat_schema)

        if is_new:
            self.create_table("table_cat", table_cat_schema, tx)
            self.create_table("field_cat", field_cat_schema, tx)

    def create_table(self, table_name: str, schema: Schema, tx: Transaction):
        # print(f"In create_table, schema's fields are: {schema.fields}")
        layout = Layout(schema)
        # print(f"creating table: {table_name}, slot_size is {layout.slot_size}")
        table_cat = TableScan(tx, "table_cat", self.__table_cat_layout)
        table_cat.insert()
        table_cat.set_string("table_name", table_name)
        table_cat.set_int("slot_size", layout.slot_size)

        field_cat = TableScan(tx, "field_cat", self.__field_cat_layout)
        # print("Start create table")
        for field_name in schema.fields:
            # print(f"table name: {table_name}, field name: {field_name}, type: {schema.get_field_type(field_name)}, length: {schema.get_field_length(field_name)}, offset: {layout.get_offset(field_name)}")
            field_cat.insert()
            field_cat.set_string("table_name", table_name)
            field_cat.set_string("field_name", field_name)
            field_cat.set_int("type", schema.get_field_type(field_name).value)
            field_cat.set_int("length", schema.get_field_length(field_name))
            field_cat.set_int("offset", layout.get_offset(field_name))
        field_cat.close()
        tx.commit()
        # print("Table Created")

    def get_layout(self, table_name: str, tx: Transaction) -> Layout:
        size = -1
        table_cat = TableScan(tx, "table_cat", self.__table_cat_layout)
        while table_cat.next():
            # print("In layout while")
            if table_cat.get_string("table_name") == table_name:
                size = table_cat.get_int("slot_size")
                # print(f"size matched {size}")
                break
        table_cat.close()

        schema = Schema()
        offsets: [str, int] = {}
        field_cat = TableScan(tx, "field_cat", self.__field_cat_layout)
        while field_cat.next():
            if field_cat.get_string("table_name") == table_name:
                field_name = field_cat.get_string("field_name")
                field_type = field_cat.get_int("type")
                field_length = field_cat.get_int("length")
                offset = field_cat.get_int("offset")
                offsets[field_name] = offset
                schema.add_field(field_name, FieldInfo(FieldType(field_type), field_length))
        field_cat.close()

        return Layout(schema, offsets, size)
