# -*- coding: utf-8 -*-
# @Time    : 2024/12/2 14:42
# @Author  : EvanWong
# @File    : FieldType.py
# @Project : TestDB

from enum import Enum


class FieldType(Enum):
    """
    Enumeration of field types supported in the database schema.

    Each field type is associated with a specific byte size:
        - INT: 4 bytes
        - STRING: Variable size, typically up to a maximum length defined elsewhere.
    """
    INT = 0  # Represents an integer field.
    FLOAT = 1 # Represents a float field.
    STRING = 2  # Represents a string field.
