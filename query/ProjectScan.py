# -*- coding: utf-8 -*-
# @Time    : 2024/12/6 17:46
# @Author  : EvanWong
# @File    : ProjectScan.py
# @Project : TestDB
from query.Constant import Constant
from query.Scan import Scan


class ProjectScan(Scan):
    def __init__(self, s: Scan, fields: list[str]):
        self.__scan = s
        self.__fields = fields

    def before_first(self):
        return self.__scan.before_first()

    def next(self) -> bool:
        # print("In s4 next")
        return self.__scan.next()

    def get_int(self, field_name: str) -> int:
        if self.has_field(field_name):
            return self.__scan.get_int(field_name)
        raise IndexError(f"Field '{field_name}' does not exist")

    def get_string(self, field_name: str) -> str:
        if self.has_field(field_name):
            return self.__scan.get_string(field_name)
        raise IndexError(f"Field '{field_name}' does not exist")

    def get_float(self, field_name: str) -> float:
        if self.has_field(field_name):
            return self.__scan.get_float(field_name)
        raise IndexError(f"Field '{field_name}' does not exist")

    def get_value(self, field_name: str) -> Constant:
        if self.has_field(field_name):
            return self.__scan.get_value(field_name)
        raise IndexError(f"Field '{field_name}' does not exist")

    def has_field(self, field_name: str) -> bool:
        return field_name in self.__fields

    def close(self):
        self.__scan.close()
