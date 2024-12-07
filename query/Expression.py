# -*- coding: utf-8 -*-
# @Time    : 2024/12/6 09:53
# @Author  : EvanWong
# @File    : Expression.py
# @Project : TestDB
from query.Constant import Constant
from query.Scan import Scan
from record.Schema import Schema


class Expression:
    def __init__(self, value: Constant=None, field_name: str=None):
        self.__value: Constant = value
        self.__field_name: str = field_name

    def __str__(self):
        return self.__field_name if self.__field_name is not None else str(self.__value)

    def evaluate(self, s: Scan) -> Constant:
        return self.__value if self.__value is not None else s.get_value(self.__field_name)

    @property
    def is_field_name(self) -> bool:
        return self.__field_name is not None

    @property
    def as_constant(self) -> Constant:
        return self.__value

    @property
    def as_field_name(self) -> str:
        return self.__field_name

    def applies_to(self, schema: Schema) -> bool:
        return self.__value is not None or schema.has_field(self.__field_name)
