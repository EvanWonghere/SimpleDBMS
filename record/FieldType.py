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
    INT = 4  # Represents an integer field with a fixed size of 4 bytes.
    STRING = 12  # Represents a string field with a fixed size of 12 bytes (this can be adjusted based on requirements).
