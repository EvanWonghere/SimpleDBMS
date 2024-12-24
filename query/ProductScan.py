# -*- coding: utf-8 -*-
# @Time    : 2024/12/6 18:09
# @Author  : EvanWong
# @File    : ProductScan.py
# @Project : TestDB

from typing import Optional
from query.Constant import Constant
from query.Scan import Scan

class ProductScan(Scan):
    """
    A scan that implements the Cartesian product of two underlying scans.

    Attributes:
        __LHS_scan (Scan): The left-hand side scan.
        __RHS_scan (Scan): The right-hand side scan.
    """

    def __init__(self, lhs_scan: Scan, rhs_scan: Scan):
        """
        Initialize a ProductScan.

        Args:
            lhs_scan (Scan): The left-hand side scan.
            rhs_scan (Scan): The right-hand side scan.
        """
        self.__LHS_scan: Scan = lhs_scan
        self.__RHS_scan: Scan = rhs_scan
        self.before_first()

    def before_first(self):
        """
        Position both scans before their first record,
        then advance the LHS to its first record.
        """
        self.__LHS_scan.before_first()
        self.__LHS_scan.next()     # Move LHS to first record
        self.__RHS_scan.before_first()

    def next(self) -> bool:
        """
        Advance to the next record in the product.

        Once the RHS is exhausted, reset it and advance the LHS.
        If the LHS is also exhausted, the product is complete.

        Returns:
            bool: True if there's another record in the product, False otherwise.
        """
        if self.__RHS_scan.next():
            return True
        self.__RHS_scan.before_first()
        if not self.__RHS_scan.next():
            # LHS exhausted or RHS has no records
            return False
        return self.__LHS_scan.next()

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
        """
        Check if either underlying scan has the specified field.

        Args:
            field_name (str): The field name to check.

        Returns:
            bool: True if the field is in LHS or RHS, False otherwise.
        """
        return self.__LHS_scan.has_field(field_name) or self.__RHS_scan.has_field(field_name)

    def close(self):
        """
        Close both underlying scans.
        """
        self.__LHS_scan.close()
        self.__RHS_scan.close()