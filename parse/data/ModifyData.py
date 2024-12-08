# -*- coding: utf-8 -*-
# @Time    : 2024/12/7 16:41
# @Author  : EvanWong
# @File    : ModifyData.py
# @Project : TestDB
from query.Expression import Expression
from query.Predicate import Predicate


class ModifyData:
    def __init__(self, table_name: str, field_name: str, new_value: Expression, predicate: Predicate):
        self.__table_name: str = table_name
        self.__field_name: str = field_name
        self.__new_value: Expression = new_value
        self.__predicate: Predicate = predicate

    @property
    def table_name(self) -> str:
        return self.__table_name

    @property
    def field_name(self) -> str:
        return self.__field_name

    @property
    def new_value(self) -> Expression:
        return self.__new_value

    @property
    def predicate(self) -> Predicate:
        return self.__predicate
