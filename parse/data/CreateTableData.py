# -*- coding: utf-8 -*-
# @Time    : 2024/12/7 16:42
# @Author  : EvanWong
# @File    : CreateTableData.py
# @Project : TestDB
from record.Schema import Schema


class CreateTableData:
    def __init__(self, table_name: str, schema: Schema):
        self.__table_name: str = table_name
        self.__schema: Schema = schema

    @property
    def table_name(self) -> str:
        return self.__table_name

    @property
    def schema(self) -> Schema:
        return self.__schema
