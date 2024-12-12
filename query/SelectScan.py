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
    def __init__(self, s: Scan, predicate: Predicate):
        self.__scan = s
        self.__predicate = predicate

    def set_int(self, field_name: str, value: int):
        us: UpdateScan = self.__scan
        us.set_int(field_name, value)

    def set_value(self, field_name: str, value: Constant):
        us: UpdateScan = self.__scan
        us.set_value(field_name, value)

    def set_float(self, field_name: str, value: float):
        us: UpdateScan = self.__scan
        us.set_float(field_name, value)

    def set_string(self, field_name: str, value: str):
        us: UpdateScan = self.__scan
        us.set_string(field_name, value)

    def insert(self):
        us: UpdateScan = self.__scan
        us.insert()

    def delete(self):
        us: UpdateScan = self.__scan
        us.delete()

    def get_rid(self) -> RID:
        us: UpdateScan = self.__scan
        return us.get_rid()

    def move_to_rid(self, rid: RID):
        us: UpdateScan = self.__scan
        us.move_to_rid(rid)

    def before_first(self):
        self.__scan.before_first()

    def next(self) -> bool:
        # print("Before s3 next")
        while self.__scan.next():
            # print("In s3 next")
            if self.__predicate.is_satisfied(self.__scan):
                # print("Scan is satisfied")
                return True
        # print("No satisfied predicate")
        return False

    def get_int(self, field_name: str) -> int:
        return self.__scan.get_int(field_name)

    def get_string(self, field_name: str) -> str:
        return self.__scan.get_string(field_name)

    def get_float(self, field_name: str) -> float:
        return self.__scan.get_float(field_name)

    def get_value(self, field_name: str) -> Constant:
        return self.__scan.get_value(field_name)

    def has_field(self, field_name: str) -> bool:
        return self.__scan.has_field(field_name)

    def close(self):
        self.__scan.close()