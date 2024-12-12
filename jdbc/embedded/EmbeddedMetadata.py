# -*- coding: utf-8 -*-
# @Time    : 2024/12/9 19:05
# @Author  : EvanWong
# @File    : EmbeddedMetadata.py
# @Project : TestDB
from record.FieldType import FieldType
from record.Schema import Schema


class EmbeddedMetadata:
    def __init__(self, schema: Schema):
        self.__schema = schema

    def get_column_count(self) -> int:
        return len(self.__schema.fields)

    def get_column_name(self, column: int) -> str:
        return self.__schema.fields[column - 1]

    def get_column_type(self, column: int) -> FieldType:
        field_name = self.get_column_name(column)
        return self.__schema.get_field_type(field_name)

    def get_column_display_size(self, column: int) -> int:
        field_name = self.get_column_name(column)
        field_type = self.get_column_type(column)
        if field_type == FieldType.INT:
            field_length = 6
        elif field_type == FieldType.FLOAT:
            field_length = 12
        else:
            field_length = self.__schema.get_field_length(field_name)
        return max(len(field_name), field_length) + 1
