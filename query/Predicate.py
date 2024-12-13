# -*- coding: utf-8 -*-
# @Time    : 2024/12/6 15:57
# @Author  : EvanWong
# @File    : Predicate.py
# @Project : TestDB
import copy
import queue
from queue import Queue
from typing import Optional

from plan.Plan import Plan
from query.Constant import Constant
from query.Scan import Scan
from query.Term import Term
from record.Schema import Schema


class Predicate:
    """
    Combination of terms.
    """
    def __init__(self, t: Term=None):
        self.__terms: list[Term] = []
        self.logic_ops: list[str] = []
        if t is not None:
            self.__terms.append(t)

    def __str__(self):
        res = ""
        if not self.__terms:
            return res

        res += str(self.__terms[0])
        for i in range(self.__terms.__len__() - 1):
            res += f" {self.logic_ops[i]} {str(self.__terms[i])}"
        return res

    def is_empty(self) -> bool:
        return len(self.__terms) == 0

    def conjoin_with(self, predicate):
        self.__terms.extend(predicate.__terms)

    def is_satisfied(self, s: Scan) -> bool:
        # for term in self.__terms:
        #     if not term.is_satisfied(s):
        #         return False
        # return True
        if len(self.__terms) == 0:
            return True

        cur_res = self.__terms[0].is_satisfied(s)
        for i in range(len(self.__terms) - 1):
            new_res = self.__terms[i+1].is_satisfied(s)
            cur_op = self.logic_ops[i]
            if cur_op == "and":
                cur_res = new_res and cur_res
            elif cur_op == "or":
                cur_res = new_res or cur_res
            else:
                raise ValueError(f"No such logic operator {cur_op}")

        return cur_res

    def reduction_factor(self, p: Plan) -> int:
        factor: int = 1
        for term in self.__terms:
            factor *= term.reduction_factor(p)
        return factor

    def select_sub_prediction(self, schema: Schema):
        res: Predicate = Predicate()
        for term in self.__terms:
            if term.applies_to(schema):
                res.__terms.append(term)
        return res if len(res.__terms) > 0 else None

    def join_sub_prediction(self, schema_1: Schema, schema_2: Schema):
        res: Predicate = Predicate()
        new_schema = Schema()
        new_schema.add_all(schema_1.fields, schema_1.infos)
        new_schema.add_all(schema_2.fields, schema_2.infos)

        for term in self.__terms:
            if (not term.applies_to(schema_1) and (not term.applies_to(schema_2))
                    and term.applies_to(new_schema)):
                res.__terms.append(term)

        return res if len(res.__terms) > 0 else None

    def equates_with_constant(self, field_name: str) -> Optional[Constant]:
        for term in self.__terms:
            c: Constant = term.equates_with_constant(field_name)
            if c is not None:
                return c
        return None

    def equates_with_field(self, field_name: str) -> Optional[str]:
        for term in self.__terms:
            c: str = term.equates_with_field(field_name)
            if c is not None:
                return c
        return None
