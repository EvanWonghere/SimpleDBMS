# -*- coding: utf-8 -*-
# @Time    : 2024/12/6 15:09
# @Author  : EvanWong
# @File    : Term.py
# @Project : TestDB
import math

from plan.Plan import Plan
from query.Constant import Constant
from query.Expression import Expression
from query.Scan import Scan
from record.Schema import Schema


class Term:
    """
    Create a new term that compares two expressions for equality.

    Attributes:
        __LHS (Expression): The left-hand-side of the expression.
        __RHS (Expression): The right-hand-side of the expression.
    """

    def __init__(self, LHS: Expression, RHS: Expression):
        self.__LHS: Expression = LHS
        self.__RHS: Expression = RHS

    def __str__(self):
        return f"{self.__LHS} -> {self.__RHS}"

    def is_satisfied(self, s: Scan) -> bool:
        lhs_value: Constant = self.__LHS.evaluate(s)
        rhs_value: Constant = self.__RHS.evaluate(s)
        # print(f"lhs_value is {lhs_value}, rhs_value is {rhs_value}, equals: {lhs_value == rhs_value}")
        return lhs_value == rhs_value

    def reduction_factor(self, p: Plan) -> int:
        if self.__LHS.is_field_name and self.__RHS.is_field_name:
            l_name = self.__LHS.as_field_name
            r_name = self.__RHS.as_field_name
            return max(p.distinct_values(l_name), p.distinct_values(r_name))
        elif self.__LHS.is_field_name or self.__RHS.is_field_name:
            name = self.__LHS.as_field_name if self.__LHS.is_field_name else self.__RHS.as_field_name
            return p.distinct_values(name)
        else:
            if self.__LHS.as_constant == self.__RHS.as_constant:
                return 1
            return int(math.inf)

    def equates_with_constant(self, field_name: str) -> Constant | None:
        # One of them must be field name and another must be constant.
        if self.__LHS.is_field_name ^ self.__RHS.is_field_name:
            if self.__LHS.is_field_name and self.__LHS.as_field_name == field_name:
                return self.__RHS.as_constant
            elif self.__RHS.is_field_name and self.__RHS.as_field_name == field_name:
                return self.__LHS.as_constant
            else:
                return None
        else:
            return None

    def equates_with_field(self, field_name: str) -> str | None:
        if self.__LHS.is_field_name and self.__RHS.is_field_name:
            if self.__LHS.as_field_name == field_name:
                return self.__RHS.as_field_name
            elif self.__RHS.as_field_name == field_name:
                return self.__LHS.as_field_name
            else:
                return None
        else:
            return None

    def applies_to(self, schema: Schema) -> bool:
        return self.__LHS.applies_to(schema) and self.__RHS.applies_to(schema)
