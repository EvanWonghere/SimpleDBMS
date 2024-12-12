# -*- coding: utf-8 -*-
# @Time    : 2024/12/6 18:09
# @Author  : EvanWong
# @File    : ProductScan.py
# @Project : TestDB
from typing import Optional

from query.Constant import Constant
from query.Scan import Scan


class ProductScan(Scan):
    def __init__(self, lhs_scan: Scan, rhs_scan: Scan):
        self.__LHS_scan: Scan = lhs_scan
        self.__RHS_scan: Scan = rhs_scan
        self.before_first()

    def next(self) -> bool:
        if self.__RHS_scan.next():
            return True
        self.__RHS_scan.before_first()
        return self.__RHS_scan.next() and self.__LHS_scan.next()

    def before_first(self):
        self.__LHS_scan.before_first()
        self.__LHS_scan.next()
        self.__RHS_scan.before_first()

    def get_int(self, field_name: str) -> Optional[int]:
        if self.__LHS_scan.has_field(field_name):
            return self.__LHS_scan.get_int(field_name)
        if self.__RHS_scan.has_field(field_name):
            return self.__RHS_scan.get_int(field_name)
        return None
    
    def get_float(self, field_name: str) -> Optional[float]:
        if self.__LHS_scan.has_field(field_name):
            return self.__LHS_scan.get_float(field_name)
        if self.__RHS_scan.has_field(field_name):
            return self.__RHS_scan.get_float(field_name)
        return None

    def get_string(self, field_name: str) -> Optional[str]:
        if self.__LHS_scan.has_field(field_name):
            return self.__LHS_scan.get_string(field_name)
        if self.__RHS_scan.has_field(field_name):
            return self.__RHS_scan.get_string(field_name)
        return None

    def get_value(self, field_name: str) -> Optional[Constant]:
        if self.__LHS_scan.has_field(field_name):
            return self.__LHS_scan.get_value(field_name)
        if self.__RHS_scan.has_field(field_name):
            return self.__RHS_scan.get_value(field_name)
        return None

    def has_field(self, field_name: str) -> bool:
        return self.__LHS_scan.has_field(field_name) or self.__RHS_scan.has_field(field_name)

    def close(self):
        self.__LHS_scan.close()
        self.__RHS_scan.close()