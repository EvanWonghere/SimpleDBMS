# -*- coding: utf-8 -*-
# @Time    : 2024/12/2 15:16
# @Author  : EvanWong
# @File    : UpdateScan.py
# @Project : TestDB
from abc import ABC, abstractmethod

from query.Constant import Constant
from query.Scan import Scan
from record.RID import RID


class UpdateScan(Scan, ABC):
    """
    Abstract base class representing a scan that allows updates to records.

    The UpdateScan interface extends the Scan interface by adding methods that
    allow modifications to the records being scanned. This includes setting field
    values, inserting new records, deleting records, and navigating to specific record IDs.
    """

    @abstractmethod
    def set_value(self, field_name: str, value: Constant):
        """
        Set the value of a specified field in the current record.

        Args:
            field_name (str): The name of the field to set.
            value (Constant): The new value to set for the field.
        """
        pass

    @abstractmethod
    def set_int(self, field_name: str, value: int):
        """
        Set an integer value for a specified field in the current record.

        Args:
            field_name (str): The name of the field to set.
            value (int): The integer value to set for the field.
        """
        pass

    @abstractmethod
    def set_string(self, field_name: str, value: str):
        """
        Set a string value for a specified field in the current record.

        Args:
            field_name (str): The name of the field to set.
            value (str): The string value to set for the field.
        """
        pass

    @abstractmethod
    def insert(self):
        """
        Insert a new record into the database.

        This method should create a new record in the underlying data store and
        make it available for subsequent operations in the scan.
        """
        pass

    @abstractmethod
    def delete(self):
        """
        Delete the current record from the database.

        This method should remove the current record from the underlying data store
        and ensure that it is no longer accessible through the scan.
        """
        pass

    @abstractmethod
    def get_rid(self) -> RID:
        """
        Retrieve the Record ID (RID) of the current record.

        Returns:
            RID: The Record ID of the current record.
        """
        pass

    @abstractmethod
    def move_to_rid(self, rid: RID):
        """
        Move the scan to a specific Record ID (RID).

        Args:
            rid (RID): The Record ID to move the scan to.
        """
        pass
