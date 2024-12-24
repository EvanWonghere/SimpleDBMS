# -*- coding: utf-8 -*-
# @Time    : 2024/12/6 18:18
# @Author  : EvanWong
# @File    : SelectScan.py
# @Project : TestDB

from query.Constant import Constant
from query.Predicate import Predicate
from query.Scan import Scan
from query.UpdateScan import UpdateScan
from record.RID import RID


class SelectScan(UpdateScan):
    """
    A scan that applies a selection predicate to an underlying base scan.

    Only those records for which the predicate is satisfied will be returned.
    This class also delegates update operations (insert/delete/set*) to the base scan
    if the underlying scan is updatable.
    """

    def __init__(self, s: Scan, predicate: Predicate):
        """
        Initialize a SelectScan with a base scan and a predicate.

        Args:
            s (Scan): The underlying scan (must be UpdateScan if updates are needed).
            predicate (Predicate): The selection predicate.
        """
        self.__scan: Scan = s
        self.__predicate: Predicate = predicate

    def before_first(self):
        """
        Position the underlying scan before the first record.
        """
        self.__scan.before_first()

    def next(self) -> bool:
        """
        Advance to the next record that satisfies the predicate.

        Returns:
            bool: True if another record is found, False if no more satisfying records exist.
        """
        while self.__scan.next():
            if self.__predicate.is_satisfied(self.__scan):
                return True
        return False

    def get_int(self, field_name: str) -> int:
        return self.__scan.get_int(field_name)

    def get_float(self, field_name: str) -> float:
        return self.__scan.get_float(field_name)

    def get_string(self, field_name: str) -> str:
        return self.__scan.get_string(field_name)

    def get_value(self, field_name: str) -> Constant:
        return self.__scan.get_value(field_name)

    def has_field(self, field_name: str) -> bool:
        return self.__scan.has_field(field_name)

    def close(self):
        self.__scan.close()

    # ----------------- UpdateScan methods ----------------- #

    def set_int(self, field_name: str, value: int):
        """
        Set an integer field value in the current record.

        Args:
            field_name (str): The field name to set.
            value (int): The integer value to assign.

        Raises:
            AttributeError: If the underlying scan is not an UpdateScan.
        """
        us = self.__get_update_scan()
        us.set_int(field_name, value)

    def set_float(self, field_name: str, value: float):
        """
        Set a float field value in the current record.

        Args:
            field_name (str): The field name to set.
            value (float): The float value to assign.
        """
        us = self.__get_update_scan()
        us.set_float(field_name, value)

    def set_string(self, field_name: str, value: str):
        """
        Set a string field value in the current record.

        Args:
            field_name (str): The field name to set.
            value (str): The string value to assign.
        """
        us = self.__get_update_scan()
        us.set_string(field_name, value)

    def set_value(self, field_name: str, value: Constant):
        """
        Set a Constant field value in the current record.

        Args:
            field_name (str): The field name to set.
            value (Constant): The new value to assign.
        """
        us = self.__get_update_scan()
        us.set_value(field_name, value)

    def insert(self):
        """
        Insert a new record into the underlying scan.
        """
        us = self.__get_update_scan()
        us.insert()

    def delete(self):
        """
        Delete the current record from the underlying scan.
        """
        us = self.__get_update_scan()
        us.delete()

    def get_rid(self) -> RID:
        us = self.__get_update_scan()
        return us.get_rid()

    def move_to_rid(self, rid: RID):
        us = self.__get_update_scan()
        us.move_to_rid(rid)

    def __get_update_scan(self) -> UpdateScan:
        """
        Retrieve the underlying scan as an UpdateScan, or raise an error if it's not updatable.

        Raises:
            AttributeError: If the underlying scan is not an UpdateScan.
        """
        if not isinstance(self.__scan, UpdateScan):
            raise AttributeError("Underlying scan is not updatable.")
        return self.__scan