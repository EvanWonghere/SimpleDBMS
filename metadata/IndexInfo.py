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
    """Information of index.
    This information could be useful to help estimate the cost of using index.

    Attributes:
        __index_name (str): The name of the index.
        __field_name(str): The name of the field.
        __tx (Transaction): The transaction that current at.
        __index_layout (layout): The layout of the index.
        __stat_info (StatInfo): The stat_info of the table.
    """

    def __init__(self, index_name: str, field_name: str, tx: Transaction,
                 table_schema: Schema,  stat_info: StatInfo):
        self.__index_name = index_name
        self.__field_name = field_name
        self.__tx: Transaction = tx
        self.__table_schema: Schema = table_schema
        # print(f"In index info initialisation, schema's fields are {self.__table_schema.fields}.")
        self.__index_layout: Layout = self.__create_index_layout()
        self.__stat_info: StatInfo = stat_info

    def open(self) -> Index:
        return HashIndex(self.__tx, self.__index_name, self.__index_layout)

    @property
    def accessed_blocks(self) -> int:
        """
        The estimation of the number of times to access blocks that to find all the index records with given key may cost.

        Returns:
            The estimated times to access blocks.
        """
        records_per_block: int = self.__tx.block_size // self.__index_layout.slot_size
        block_num: int = self.__stat_info.output_records // records_per_block
        return HashIndex.search_cost(block_num)

    @property
    def output_records(self) -> int:
        return self.__stat_info.output_records // self.__stat_info.distinct_values

    def distinct_values(self, field_name: str) -> int:
        return 1 if field_name == self.__field_name else self.__stat_info.distinct_values

    def __create_index_layout(self) -> Layout:
        schema = Schema()
        schema.add_int_field("block")
        schema.add_int_field("id")

        if self.__table_schema.get_field_type(self.__field_name) is FieldType.INT:
            schema.add_int_field("data_value")
        elif self.__table_schema.get_field_type(self.__field_name) is FieldType.FLOAT:
            schema.add_float_field("data_value")
        else:
            length = self.__table_schema.get_field_length(self.__field_name)
            schema.add_string_field("data_value", length)

        return Layout(schema)
