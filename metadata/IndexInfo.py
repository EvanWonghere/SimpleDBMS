# -*- coding: utf-8 -*-
# @Time    : 2024/12/2 17:16
# @Author  : EvanWong
# @File    : IndexInfo.py
# @Project : TestDB

from index.Index import Index
from index.hash.HashIndex import HashIndex
from metadata.StatInfo import StatInfo
from record.FieldType import FieldType
from record.Layout import Layout
from record.Schema import Schema
from tx.Transaction import Transaction


class IndexInfo:
    """
    Holds metadata about a specific index, such as its name, field, layout, and statistical information.

    This information is used to estimate the cost of using the index and to open the index
    for inserting, deleting, or searching records.

    Attributes:
        __index_name (str): The name of the index.
        __field_name (str): The name of the field on which the index is built.
        __tx (Transaction): The current transaction.
        __table_schema (Schema): The schema of the table on which this index is built.
        __index_layout (Layout): The layout for the index's structure.
        __stat_info (StatInfo): The statistical information for the table.
    """

    def __init__(self, index_name: str, field_name: str, tx: Transaction,
                 table_schema: Schema, stat_info: StatInfo):
        """
        Initialize an IndexInfo instance.

        Args:
            index_name (str): The name of the index.
            field_name (str): The name of the field.
            tx (Transaction): The current transaction.
            table_schema (Schema): The schema of the associated table.
            stat_info (StatInfo): Statistical information about the table.
        """
        self.__index_name: str = index_name
        self.__field_name: str = field_name
        self.__tx: Transaction = tx
        self.__table_schema: Schema = table_schema
        self.__stat_info: StatInfo = stat_info
        self.__index_layout: Layout = self.__create_index_layout()

    def open(self) -> Index:
        """
        Open the index for operations such as insert, delete, or search.

        Returns:
            Index: An instance of the index. (e.g., HashIndex or B-Tree index)
        """
        return HashIndex(self.__tx, self.__index_name, self.__index_layout)

    @property
    def accessed_blocks(self) -> int:
        """
        Estimate the cost (in block accesses) of searching all index records with a given key.

        Returns:
            int: An estimated number of block accesses when using this index.
        """
        records_per_block: int = self.__tx.block_size // self.__index_layout.slot_size
        if records_per_block == 0:
            return 1  # Avoid division by zero, assume at least one block access
        block_num: int = self.__stat_info.output_records // records_per_block
        return HashIndex.search_cost(block_num)

    @property
    def output_records(self) -> int:
        """
        Estimate the number of output records for this index,
        typically used when planning queries.

        Returns:
            int: An estimated number of output records.
        """
        distinct = self.__stat_info.distinct_values
        if distinct == 0:
            return self.__stat_info.output_records
        return self.__stat_info.output_records // distinct

    def distinct_values(self, field_name: str) -> int:
        """
        Estimate the number of distinct values for the given field.
        If the field matches the index field, assume 1 distinct value
        (i.e., a highly selective index).

        Args:
            field_name (str): The name of the field.

        Returns:
            int: The number of distinct values for the specified field.
        """
        return 1 if field_name == self.__field_name else self.__stat_info.distinct_values

    def __create_index_layout(self) -> Layout:
        """
        Create a layout for the index, which includes fields:
            - block (int)
            - id (int)
            - data_value (int / float / string)

        Returns:
            Layout: The layout describing how index records are stored.
        """
        schema = Schema()
        schema.add_int_field("block")
        schema.add_int_field("id")

        field_type = self.__table_schema.get_field_type(self.__field_name)
        if field_type == FieldType.INT:
            schema.add_int_field("data_value")
        elif field_type == FieldType.FLOAT:  # If you have a FLOAT type
            schema.add_float_field("data_value")  # Make sure Schema supports add_float_field
        else:
            length = self.__table_schema.get_field_length(self.__field_name)
            schema.add_string_field("data_value", length)

        return Layout(schema)