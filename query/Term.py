# -*- coding: utf-8 -*-
# @Time    : 2024/12/6 15:09
# @Author  : EvanWong
# @File    : Term.py
# @Project : TestDB

import math
from typing import Optional

from plan.Plan import Plan
from query.Constant import Constant
from query.Expression import Expression
from query.Scan import Scan
from record.Schema import Schema


class Term:
    """
    A term in a predicate, comparing two expressions for equality.

    Attributes:
        __LHS (Expression): The left-hand side expression.
        __RHS (Expression): The right-hand side expression.
    """

    def __init__(self, lhs: Expression, rhs: Expression):
        """
        Initialize a Term with two expressions to compare.

        Args:
            lhs (Expression): The left-hand side expression.
            rhs (Expression): The right-hand side expression.
        """
        self.__LHS: Expression = lhs
        self.__RHS: Expression = rhs

    def __str__(self) -> str:
        """
        Return a string representation of the term (LHS = RHS).

        Returns:
            str: A string in the format "LHS = RHS".
        """
        return f"{self.__LHS} = {self.__RHS}"

    def is_satisfied(self, s: Scan) -> bool:
        """
        Check if this term is satisfied by the current record in the scan.

        Args:
            s (Scan): The current scan.

        Returns:
            bool: True if LHS == RHS in the context of the scan, False otherwise.
        """
        lhs_value: Constant = self.__LHS.evaluate(s)
        rhs_value: Constant = self.__RHS.evaluate(s)
        return lhs_value == rhs_value

    def reduction_factor(self, p: Plan) -> int:
        """
        Estimate the reduction factor of applying this term to a plan.

        - If both LHS and RHS are field names, return the max of distinct_values for each field.
        - If one is a field name and the other is a constant, return distinct_values of that field.
        - If both are constants:
            - If they are equal, return 1 (selectivity is high).
            - Otherwise, return infinity (term is never satisfied).

        Args:
            p (Plan): The plan to estimate on.

        Returns:
            int: The estimated reduction factor.
        """
        if self.__LHS.is_field_name and self.__RHS.is_field_name:
            l_name = self.__LHS.as_field_name
            r_name = self.__RHS.as_field_name
            return max(p.distinct_values(l_name), p.distinct_values(r_name))
        elif self.__LHS.is_field_name or self.__RHS.is_field_name:
            field_name = (self.__LHS.as_field_name
                          if self.__LHS.is_field_name
                          else self.__RHS.as_field_name)
            return p.distinct_values(field_name)
        else:
            # both are constants
            return 1 if self.__LHS.as_constant == self.__RHS.as_constant else math.inf

    def equates_with_constant(self, field_name: str) -> Optional[Constant]:
        """
        Check if this term equates the specified field with a constant.

        If LHS or RHS is the given field name and the other side is a constant,
        return that constant. Otherwise, return None.

        Args:
            field_name (str): The field name to check.

        Returns:
            Optional[Constant]: The constant if the term is field = constant, else None.
        """
        # XOR: one expression must be a field, the other must be a constant
        if self.__LHS.is_field_name ^ self.__RHS.is_field_name:
            if self.__LHS.is_field_name and self.__LHS.as_field_name == field_name:
                return self.__RHS.as_constant
            if self.__RHS.is_field_name and self.__RHS.as_field_name == field_name:
                return self.__LHS.as_constant
        return None

    def equates_with_field(self, field_name: str) -> Optional[str]:
        """
        Check if this term equates the specified field with another field.

        If LHS or RHS is the given field name, return the other field name.
        Otherwise, return None.

        Args:
            field_name (str): The field name to check.

        Returns:
            Optional[str]: The other field name if field_name is equated with a field, else None.
        """
        if self.__LHS.is_field_name and self.__RHS.is_field_name:
            if self.__LHS.as_field_name == field_name:
                return self.__RHS.as_field_name
            if self.__RHS.as_field_name == field_name:
                return self.__LHS.as_field_name
        return None

    def applies_to(self, schema: Schema) -> bool:
        """
        Check if both expressions in this term apply to the given schema.

        Args:
            schema (Schema): The schema to check against.

        Returns:
            bool: True if both LHS and RHS apply to the schema, False otherwise.
        """
        return self.__LHS.applies_to(schema) and self.__RHS.applies_to(schema)