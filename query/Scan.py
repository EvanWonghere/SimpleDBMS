# -*- coding: utf-8 -*-
# @Time    : 2024/12/2 15:11
# @Author  : EvanWong
# @File    : Scan.py
# @Project : TestDB
from abc import ABC, abstractmethod

from query.Constant import Constant


class Scan(ABC):
    """
    Abstract base class representing a scan over a set of records.

    The Scan interface defines the methods required to iterate over a set of records
    and retrieve their values. It serves as the foundation for implementing various
    types of scans, such as sequential scans, index scans, etc.
    """

    @abstractmethod
    def before_first(self):
        """
        Position the scan before the first record.

        This method resets the scan so that the next call to `next()` will position
        it at the first record in the scan.
        """
        pass

    @abstractmethod
    def next(self) -> bool:
        """
        Advance the scan to the next record.

        Returns:
            bool: True if there is a next record to read, False if the end of the scan is reached.
        """
        pass

    @abstractmethod
    def get_int(self, field_name: str) -> int:
        """
        Retrieve an integer value from the current record.

        Args:
            field_name (str): The name of the field to retrieve the integer value from.

        Returns:
            int: The integer value of the specified field.

        Raises:
            KeyError: If the field name does not exist.
            ValueError: If the field value is not an integer.
        """
        pass

    @abstractmethod
    def get_str(self, field_name: str) -> str:
        """
        Retrieve a string value from the current record.

        Args:
            field_name (str): The name of the field to retrieve the string value from.

        Returns:
            str: The string value of the specified field.

        Raises:
            KeyError: If the field name does not exist.
            ValueError: If the field value is not a string.
        """
        pass

    @abstractmethod
    def get_value(self, field_name: str) -> Constant:
        """
        Retrieve the value from the current record as a Constant.

        Args:
            field_name (str): The name of the field to retrieve the value from.

        Returns:
            Constant: The value of the specified field encapsulated in a Constant object.

        Raises:
            KeyError: If the field name does not exist.
        """
        pass

    @abstractmethod
    def has_field(self, field_name: str) -> bool:
        """
        Check if the current record contains a specified field.

        Args:
            field_name (str): The name of the field to check.

        Returns:
            bool: True if the field exists in the current record, False otherwise.
        """
        pass

    @abstractmethod
    def close(self):
        """
        Close the scan and release any associated resources.

        This method should be called when the scan is no longer needed to ensure that
        resources are properly released.
        """
        pass
