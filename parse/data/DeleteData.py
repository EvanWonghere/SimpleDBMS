# -*- coding: utf-8 -*-
# @Time    : 2024/12/7 16:41
# @Author  : EvanWong
# @File    : DeleteData.py
# @Project : TestDB
from query.Predicate import Predicate


class DeleteData:
    def __init__(self, table_name: str, predicate: Predicate):
        self.__table_name: str = table_name
        self.__predicate: Predicate = predicate

    @property
    def table_name(self) -> str:
        return self.__table_name

    @property
    def predicate(self) -> Predicate:
        return self.__predicate
