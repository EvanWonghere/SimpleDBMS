# -*- coding: utf-8 -*-
# @Time    : 2024/12/2 17:14
# @Author  : EvanWong
# @File    : TableMgr.py
# @Project : TestDB

from typing import Dict
from record.FieldType import FieldType
from record.Layout import Layout
from record.Schema import Schema, FieldInfo
from record.TableScan import TableScan
from tx.Transaction import Transaction

# TODO: Implement adding columns to an existing table in the future.

class TableMgr:
    """
    Manages the creation of tables and storage of their metadata in the catalog tables ('table_cat' and 'field_cat').

    Attributes:
        MAX_NAME_LENGTH (int): The maximum length of the table name.
        __table_cat_layout (Layout): The layout for the 'table_cat' catalog table.
        __field_cat_layout (Layout): The layout for the 'field_cat' catalog table.
    """

    MAX_NAME_LENGTH = 16

    def __init__(self, is_new: bool, tx: Transaction):
        """
        Initialize the TableMgr. If it's a new database, create the catalog tables.

        Args:
            is_new (bool): Indicates whether the database is new.
            tx (Transaction): The current transaction.
        """
        # Create schema for 'table_cat'
        table_cat_schema = Schema()
        table_cat_schema.add_string_field("table_name", self.MAX_NAME_LENGTH)
        table_cat_schema.add_int_field("slot_size")
        self.__table_cat_layout: Layout = Layout(table_cat_schema)

        # Create schema for 'field_cat'
        field_cat_schema = Schema()
        field_cat_schema.add_string_field("table_name", self.MAX_NAME_LENGTH)
        field_cat_schema.add_string_field("field_name", self.MAX_NAME_LENGTH)
        field_cat_schema.add_int_field("type")
        field_cat_schema.add_int_field("length")
        field_cat_schema.add_int_field("offset")
        self.__field_cat_layout: Layout = Layout(field_cat_schema)

        # Create the catalog tables if the database is new
        if is_new:
            self.create_table("table_cat", table_cat_schema, tx)
            self.create_table("field_cat", field_cat_schema, tx)

    def create_table(self, table_name: str, schema: Schema, tx: Transaction):
        """
        Create a new table with the given schema, and record its metadata in 'table_cat' and 'field_cat'.

        Args:
            table_name (str): The name of the new table.
            schema (Schema): The schema defining the table's structure.
            tx (Transaction): The current transaction.

        Raises:
            ValueError: If a table with the given name already exists.
        """
        layout = Layout(schema)
        table_cat = TableScan(tx, "table_cat", self.__table_cat_layout)
        table_cat.before_first()

        # Check if table already exists
        while table_cat.next():
            if table_cat.get_string("table_name") == table_name:
                table_cat.close()
                raise ValueError(f"Table '{table_name}' already exists.")

        # Insert a new record into 'table_cat'
        table_cat.insert()
        table_cat.set_string("table_name", table_name)
        table_cat.set_int("slot_size", layout.slot_size)
        table_cat.close()

        # Insert field info into 'field_cat'
        field_cat = TableScan(tx, "field_cat", self.__field_cat_layout)
        for field_name in schema.fields:
            field_type_value = schema.get_field_type(field_name).value
            field_length = schema.get_field_length(field_name)
            offset = layout.get_offset(field_name)

            field_cat.insert()
            field_cat.set_string("table_name", table_name)
            field_cat.set_string("field_name", field_name)
            field_cat.set_int("type", field_type_value)
            field_cat.set_int("length", field_length)
            field_cat.set_int("offset", offset)
        field_cat.close()

    def get_layout(self, table_name: str, tx: Transaction) -> Layout:
        """
        Retrieve the layout (schema and field offsets) for the specified table.

        Args:
            table_name (str): The name of the table.
            tx (Transaction): The current transaction.

        Returns:
            Layout: The layout describing the table's structure.

        Raises:
            ValueError: If the table is not found in 'table_cat'.
        """
        size = -1
        table_cat = TableScan(tx, "table_cat", self.__table_cat_layout)
        while table_cat.next():
            if table_cat.get_string("table_name") == table_name:
                size = table_cat.get_int("slot_size")
                break
        table_cat.close()

        if size == -1:
            raise ValueError(f"Table '{table_name}' does not exist.")

        schema = Schema()
        offsets: Dict[str, int] = {}

        field_cat = TableScan(tx, "field_cat", self.__field_cat_layout)
        while field_cat.next():
            if field_cat.get_string("table_name") == table_name:
                field_name = field_cat.get_string("field_name")
                field_type_val = field_cat.get_int("type")
                field_length = field_cat.get_int("length")
                offset = field_cat.get_int("offset")

                offsets[field_name] = offset
                schema.add_field(field_name, FieldInfo(FieldType(field_type_val), field_length))
        field_cat.close()

        return Layout(schema, offsets, size)