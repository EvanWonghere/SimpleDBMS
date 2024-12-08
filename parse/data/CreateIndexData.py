# -*- coding: utf-8 -*-
# @Time    : 2024/12/7 17:05
# @Author  : EvanWong
# @File    : CreateIndexData.py
# @Project : TestDB


class CreateIndexData:
    def __init__(self, index_name: str, table_name: str, field_name: str):
        self.__index_name: str = index_name
        self.__table_name: str = table_name
        self.__field_name: str = field_name

    @property
    def index_name(self) -> str:
        return self.__index_name

    @property
    def table_name(self) -> str:
        return self.__table_name

    @property
    def field_name(self) -> str:
        return self.__field_name
