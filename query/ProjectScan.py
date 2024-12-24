# -*- coding: utf-8 -*-
# @Time    : 2024/12/6 17:46
# @Author  : EvanWong
# @File    : ProjectScan.py
# @Project : TestDB

from query.Constant import Constant
from query.Scan import Scan

class ProjectScan(Scan):
    """
    A scan that projects only a specified list of fields from the underlying scan.

    Attributes:
        __scan (Scan): The underlying scan.
        __fields (list[str]): The list of fields to project.
    """

    def __init__(self, s: Scan, fields: list[str]):
        """
        Initialize a ProjectScan.

        Args:
            s (Scan): The underlying scan.
            fields (list[str]): The fields to project.
        """
        self.__scan: Scan = s
        self.__fields: list[str] = fields

    def before_first(self):
        """
        Position the underlying scan before its first record.
        """
        self.__scan.before_first()

    def next(self) -> bool:
        """
        Move to the next record in the underlying scan.

        Returns:
            bool: True if there is a next record, otherwise False.
        """
        return self.__scan.next()

    def get_int(self, field_name: str) -> int:
        if self.has_field(field_name):
            return self.__scan.get_int(field_name)
        raise KeyError(f"Field '{field_name}' does not exist in projection")

    def get_float(self, field_name: str) -> float:
        if self.has_field(field_name):
            return self.__scan.get_float(field_name)
        raise KeyError(f"Field '{field_name}' does not exist in projection")

    def get_string(self, field_name: str) -> str:
        if self.has_field(field_name):
            return self.__scan.get_string(field_name)
        raise KeyError(f"Field '{field_name}' does not exist in projection")

    def get_value(self, field_name: str) -> Constant:
        if self.has_field(field_name):
            return self.__scan.get_value(field_name)
        raise KeyError(f"Field '{field_name}' does not exist in projection")

    def has_field(self, field_name: str) -> bool:
        """
        Check if the specified field is in the projection list.

        Args:
            field_name (str): The field name to check.

        Returns:
            bool: True if the field is projected, otherwise False.
        """
        return field_name in self.__fields

    def close(self):
        """
        Close the underlying scan.
        """
        self.__scan.close()