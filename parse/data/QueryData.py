# -*- coding: utf-8 -*-
# @Time    : 2024/12/7 16:42
# @Author  : EvanWong
# @File    : QueryData.py
# @Project : TestDB
from typing import Collection

from query.Predicate import Predicate


class QueryData:
    def __init__(self, fields: list[str], tables: Collection[str], predicate: Predicate):
        self.__fields: list[str] = fields
        self.__tables: Collection[str] = tables
        self.__predicate: Predicate = predicate

    def __str__(self) -> str:
        fields = ", ".join(self.__fields)
        tables = ", ".join(self.__tables)
        predicate = f" WHERE {self.__predicate}" if self.__predicate else ""
        return f"select {fields} from {tables}{predicate}"

    @property
    def fields(self) -> list[str]:
        return self.__fields

    @property
    def tables(self) -> Collection[str]:
        return self.__tables

    @property
    def predicate(self) -> Predicate:
        return self.__predicate
