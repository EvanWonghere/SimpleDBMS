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
    def __init__(self, index_name: str, field_name: str, tx: Transaction,
                 table_schema: Schema,  stat_info: StatInfo):
        self.__index_name = index_name
        self.__field_name = field_name
        self.__tx = tx
        self.__index_layout = self.__create_index_layout()
        self.__table_schema = table_schema
        self.__stat_info = stat_info

    def open(self) -> Index:
        return HashIndex(self.__tx, self.__index_name, self.__index_layout)

    @property
    def accessed_blocks(self) -> int:
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

        if schema.get_field_type(self.__field_name) is FieldType.INT:
            schema.add_int_field("data_value")
        else:
            length = self.__table_schema.get_field_length(self.__field_name)
            schema.add_string_field("data_value", length)

        return Layout(schema)
