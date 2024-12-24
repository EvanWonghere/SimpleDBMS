# -*- coding: utf-8 -*-
# @Time    : 2024/12/2 14:53
# @Author  : EvanWong
# @File    : Constant.py
# @Project : TestDB

from typing import Union


class Constant:
    """
    Represents a value stored in the database, which can be an integer, float, or string.

    This class encapsulates a single value that can be of type `int`, `float`, or `str`.
    It provides methods to retrieve the value in its respective type.

    Attributes:
        __int_value (int | None): The integer value if the stored value is an int; otherwise, None.
        __float_value (float | None): The float value if the stored value is a float; otherwise, None.
        __str_value (str | None): The string value if the stored value is a str; otherwise, None.
    """

    def __init__(self, value: Union[int, float, str]):
        """
        Initialize a Constant with either an integer, float, or string value.

        Args:
            value (Union[int, float, str]): The value to be stored as a Constant.

        Raises:
            TypeError: If the value is not an int, float, or str.
        """
        if isinstance(value, int):
            self.__int_value = value
            self.__float_value = None
            self.__str_value = None
        elif isinstance(value, float):
            self.__int_value = None
            self.__float_value = value
            self.__str_value = None
        elif isinstance(value, str):
            self.__int_value = None
            self.__float_value = None
            self.__str_value = value
        else:
            raise TypeError("Constant value must be int, float, or str.")

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
        raise ValueError("Stored value is not an integer")

    def as_float(self) -> float:
        """
        Retrieve the stored value as a float.

        Returns:
            float: The float value stored in the Constant.

        Raises:
            ValueError: If the stored value is not a float.
        """
        if self.__float_value is not None:
            return self.__float_value
        raise ValueError("Stored value is not a float")

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
        raise ValueError("Stored value is not a string")

    def __eq__(self, other) -> bool:
        """
        Check equality between two Constant instances based on their stored values.

        Args:
            other (Constant): The other Constant instance to compare with.

        Returns:
            bool: True if both constants have the same value and type, False otherwise.
        """
        if not isinstance(other, Constant):
            return NotImplemented
        return (self.__int_value == other.__int_value
                and self.__float_value == other.__float_value
                and self.__str_value == other.__str_value)

    def __lt__(self, other) -> bool:
        """
        Check if this Constant is less than another Constant.
        Comparison logic:
            - If both values are int, compare as integers.
            - If both values are float, compare as floats.
            - If both values are str, compare lexicographically.
            - If comparing int and float, convert int to float or vice versa.
            - Otherwise, default to NotImplemented or a custom rule (e.g., int < str < float).

        Args:
            other (Constant): The other Constant instance to compare with.

        Returns:
            bool: True if this Constant is less than the other, False otherwise.
        """
        if not isinstance(other, Constant):
            return NotImplemented

        # Both int
        if self.__int_value is not None and other.__int_value is not None:
            return self.__int_value < other.__int_value

        # Both float
        if self.__float_value is not None and other.__float_value is not None:
            return self.__float_value < other.__float_value

        # Both str
        if self.__str_value is not None and other.__str_value is not None:
            return self.__str_value < other.__str_value

        # int vs float
        if self.__int_value is not None and other.__float_value is not None:
            return float(self.__int_value) < other.__float_value
        if self.__float_value is not None and other.__int_value is not None:
            return self.__float_value < float(other.__int_value)

        # If we reach here, we can define a custom fallback (like int < str < float)
        # or simply return NotImplemented. For demonstration, let's return NotImplemented:
        return NotImplemented

    def __hash__(self) -> int:
        """
        Compute the hash of the Constant based on the stored value.

        Returns:
            int: The hash of the stored value.
        """
        if self.__int_value is not None:
            return hash(self.__int_value)
        elif self.__float_value is not None:
            return hash(self.__float_value)
        elif self.__str_value is not None:
            return hash(self.__str_value)
        return 0  # fallback, though this case shouldn't occur

    def __str__(self) -> str:
        """
        Return a string representation of the Constant.

        Returns:
            str: A string representation of the stored value.
        """
        if self.__int_value is not None:
            return str(self.__int_value)
        if self.__float_value is not None:
            return str(self.__float_value)
        return self.__str_value