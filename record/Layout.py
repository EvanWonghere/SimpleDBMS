# -*- coding: utf-8 -*-
# @Time    : 2024/11/30 11:40
# @Author  : EvanWong
# @File    : Layout.py
# @Project : TestDB

from typing import Dict, Optional

from file.Page import Page
from record.FieldType import FieldType
from record.Schema import Schema


class Layout:
    """
    Describes the structure of a record within a table.

    The Layout class provides the mapping of each field's name to its byte offset within a record.
    It also calculates the total size of a record (slot size) based on the schema.

    Attributes:
        __schema (Schema): The schema of the record, defining field names and types.
        __offset (Dict[str, int]): A mapping from field names to their byte offsets within a record.
        __slot_size (int): The total size of a record slot in bytes.
    """

    def __init__(self, schema: Schema, offset: Optional[Dict[str, int]] = None, slot_size: Optional[int] = None):
        """
        Initialize the Layout with a given schema and optionally predefined offsets and slot size.

        There are two constructors:
            1. If `offset` or `slot_size` is not provided, compute offsets based on the schema.
            2. If both `offset` and `slot_size` are provided, use them directly.

        Args:
            schema (Schema): The schema of the record.
            offset (Optional[Dict[str, int]]): Predefined mapping of field names to byte offsets.
            slot_size (Optional[int]): Predefined size of a record slot in bytes.

        Raises:
            ValueError: If only one of `offset` or `slot_size` is provided without the other.
        """
        self.__schema: Schema = schema
        self.__offset: Dict[str, int] = {}
        self.__slot_size: int = 0

        # If both offset and slot_size are provided, use them
        if (offset is not None) and (slot_size is not None):
            self.__offset = offset
            self.__slot_size = slot_size
        elif (offset is None) and (slot_size is None):
            # Compute offsets based on the schema
            pos = 4  # Starting position after the flag (assuming 4 bytes for flag)
            for field in self.__schema.fields:
                self.__offset[field] = pos
                pos += self.__length_in_bytes(field)
            self.__slot_size = pos
        else:
            raise ValueError("Both 'offset' and 'slot_size' must be provided together or omitted together.")

    @property
    def schema(self) -> Schema:
        """
        Get the schema associated with this layout.

        Returns:
            Schema: The schema of the record.
        """
        return self.__schema

    def get_offset(self, field_name: str) -> int:
        """
        Get the byte offset of a specified field within a record.

        Args:
            field_name (str): The name of the field.

        Returns:
            int: The byte offset of the field.

        Raises:
            KeyError: If the field name does not exist in the schema.
        """
        if field_name in self.__offset:
            # print(f"In layout, getting offset for field {field_name}, offset is {self.__offset[field_name]}")
            return self.__offset[field_name]
        else:
            raise KeyError(f"Field '{field_name}' does not exist in the layout.")

    @property
    def slot_size(self) -> int:
        """
        Get the total size of a record slot in bytes.

        Returns:
            int: The size of the slot.
        """
        return self.__slot_size

    def __length_in_bytes(self, field_name: str) -> int:
        """
        Calculate the byte length of a specified field based on its type and length.

        Args:
            field_name (str): The name of the field.

        Returns:
            int: The byte length of the field.

        Raises:
            KeyError: If the field name does not exist in the schema.
        """
        field_type = self.__schema.get_field_type(field_name)
        field_length = self.__schema.get_field_length(field_name)
        if field_type == FieldType.INT:
            return 4  # Fixed size for integer fields
        elif field_type == FieldType.STRING:
            return Page.max_length(field_length)  # Variable size for string fields
        else:
            raise ValueError(f"Unsupported FieldType '{field_type}' for field '{field_name}'.")
