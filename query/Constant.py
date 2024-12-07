# -*- coding: utf-8 -*-
# @Time    : 2024/12/2 14:53
# @Author  : EvanWong
# @File    : Constant.py
# @Project : TestDB
from typing import Union


class Constant:
    """
    Represents a value stored in the database, which can be either an integer or a string.

    This class encapsulates a single value that can be of type `int` or `str`.
    It provides methods to retrieve the value in its respective type.

    Attributes:
        __int_value (int | None): The integer value if the stored value is an int; otherwise, None.
        __str_value (str | None): The string value if the stored value is a str; otherwise, None.
    """
    def __init__(self, value: Union[int, str]):
        """
        Initialize a Constant with either an integer or a string value.

        Args:
            value (Union[int, str]): The value to be stored as a Constant.

        Raises:
            TypeError: If the value is neither an int nor a str.
        """
        if isinstance(value, int):
            self.__int_value = value
            self.__str_value = None
        elif isinstance(value, str):
            self.__int_value = None
            self.__str_value = value
        else:
            raise TypeError("Constant value must be either int or str")

    def as_int(self) -> int:
        """
        Retrieve the stored value as an integer.

        Returns:
            int: The integer value stored in the Constant.

        Raises:
            ValueError: If the stored value is not an integer.
        """
        if self.__int_value is not None:
            return self.__int_value
        else:
            raise ValueError("Stored value is not an integer")

    def as_str(self) -> str:
        """
        Retrieve the stored value as a string.

        Returns:
            str: The string value stored in the Constant.

        Raises:
            ValueError: If the stored value is not a string.
        """
        if self.__str_value is not None:
            return self.__str_value
        else:
            raise ValueError("Stored value is not a string")

    def __eq__(self, other) -> bool:
        """
        Check equality between two Constant instances.

        Args:
            other (Constant): The other Constant instance to compare with.

        Returns:
            bool: True if both constants have the same value and type, False otherwise.
        """
        if isinstance(other, Constant):
            return self.__int_value == other.__int_value if self.__int_value is not None else self.__str_value == other.__str_value
        return False
    def __lt__(self, other) -> bool:
        """
        Check if this Constant is less than another Constant.

        Comparison is based on the value type:
            - Integers are compared numerically.
            - Strings are compared lexicographically.
            - Integers are considered less than strings.

        Args:
            other (Constant): The other Constant instance to compare with.

        Returns:
            bool: True if this Constant is less than the other, False otherwise.

        Raises:
            TypeError: If the other object is not a Constant.
        """
        if not isinstance(other, Constant):
            return NotImplemented
        if self.__int_value is not None and other.__int_value is not None:
            return self.__int_value < other.__int_value
        elif self.__str_value is not None and other.__str_value is not None:
            return self.__str_value < other.__str_value
        elif self.__int_value is not None and other.__str_value is not None:
            return True  # Define that int < str
        elif self.__str_value is not None and other.__int_value is not None:
            return False  # str > int
        else:
            return False

    def __hash__(self) -> int:
        """
        Compute the hash of the Constant.

        The hash is based on the stored value.

        Returns:
            int: The hash of the stored value.
        """
        return hash(self.__int_value) if self.__int_value is not None else hash(self.__str_value)

    def __str__(self) -> str:
        """
        Return the string representation of the Constant.

        Returns:
            str: The string representation of the stored value.
        """
        return str(self.__int_value) if self.__int_value is not None else self.__str_value
