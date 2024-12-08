# -*- coding: utf-8 -*-
# @Time    : 2024/12/7 16:40
# @Author  : EvanWong
# @File    : InsertData.py
# @Project : TestDB
from query.Constant import Constant


class InsertData:
    def __init__(self, table_name: str, fields: list[str], values: list[Constant]):
        self.__table_name: str = table_name
        self.__fields: list[str] = fields
        self.__values: list[Constant] = values

    @property
    def tabel_name(self) -> str:
        return self.__table_name

    @property
    def fields(self) -> list[str]:
        return self.__fields

    @property
    def values(self) -> list[Constant]:
        return self.__values
