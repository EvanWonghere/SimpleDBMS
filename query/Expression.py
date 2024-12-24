# -*- coding: utf-8 -*-
# @Time    : 2024/12/6 09:53
# @Author  : EvanWong
# @File    : Expression.py
# @Project : TestDB

from query.Constant import Constant
from query.Scan import Scan
from record.Schema import Schema


class Expression:
    """
    Represents a SQL expression, which can be either a constant value or a field name.

    Attributes:
        __value (Constant): A constant value for the expression, if any.
        __field_name (str): The name of the field, if the expression is a reference to a field.
    """

    def __init__(self, value: Constant = None, field_name: str = None):
        """
        Initialize the Expression. Exactly one of `value` or `field_name` should be non-None.

        Args:
            value (Constant, optional): A constant value for the expression.
            field_name (str, optional): The name of the field for the expression.

        Raises:
            ValueError: If both `value` and `field_name` are None, or both are not None.
        """
        if value is not None and field_name is not None:
            raise ValueError("Expression can only hold either a Constant or a field name, not both.")
        if value is None and field_name is None:
            raise ValueError("Either a Constant or a field name must be provided.")
        self.__value: Constant = value
        self.__field_name: str = field_name

    def __str__(self) -> str:
        """
        Return a string representation of the expression.

        Returns:
            str: The field name if it's a field reference, otherwise the string of the constant.
        """
        return self.__field_name if self.__field_name is not None else str(self.__value)

    def evaluate(self, s: Scan) -> Constant:
        """
        Evaluate the expression in the context of a given scan.

        If the expression is a field reference, retrieve the field's value from the scan.
        Otherwise, return the constant.

        Args:
            s (Scan): The current scan.

        Returns:
            Constant: The evaluated value.
        """
        if self.__value is not None:
            return self.__value
        return s.get_value(self.__field_name)

    @property
    def is_field_name(self) -> bool:
        """
        Check if this expression represents a field name.

        Returns:
            bool: True if the expression is a field reference, False if it's a constant.
        """
        return self.__field_name is not None

    @property
    def as_constant(self) -> Constant:
        """
        Retrieve the expression as a constant (if it is one).

        Returns:
            Constant: The stored constant.

        Raises:
            ValueError: If the expression is not a constant.
        """
        if self.__value is None:
            raise ValueError("This Expression does not hold a Constant.")
        return self.__value

    @property
    def as_field_name(self) -> str:
        """
        Retrieve the expression as a field name (if it is one).

        Returns:
            str: The field name.

        Raises:
            ValueError: If the expression is not a field reference.
        """
        if self.__field_name is None:
            raise ValueError("This Expression does not hold a field name.")
        return self.__field_name

    def applies_to(self, schema: Schema) -> bool:
        """
        Check if this expression applies to a given schema.

        If it's a constant, it trivially applies; if it's a field,
        check if that field is present in the schema.

        Args:
            schema (Schema): The table schema.

        Returns:
            bool: True if the expression can be evaluated against the given schema.
        """
        if self.__value is not None:
            return True
        return schema.has_field(self.__field_name)