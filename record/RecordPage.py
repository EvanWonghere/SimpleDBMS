# -*- coding: utf-8 -*-
# @Time    : 2024/11/30 11:40
# @Author  : EvanWong
# @File    : RecordPage.py
# @Project : TestDB

from file.BlockID import BlockID
from record.FieldType import FieldType
from record.Layout import Layout
from tx.Transaction import Transaction


class RecordPage:
    """
    Manages records within a specific block.

    This class provides methods to manipulate records stored in a block,
    including setting and getting integer and string fields, deleting records,
    formatting the block, and navigating through slots.

    Attributes:
        EMPTY (int): Identifier for an empty slot.
        USED (int): Identifier for a used slot.
    """

    EMPTY = 0  # Flag indicating the slot is empty
    USED = 1  # Flag indicating the slot is used

    def __init__(self, tx: Transaction, blk: BlockID, layout: Layout):
        """
        Initialize a RecordPage with a transaction, block ID, and layout.

        Args:
            tx (Transaction): The transaction managing this RecordPage.
            blk (BlockID): The block identifier.
            layout (Layout): The layout of the records in the block.
        """
        self.__tx = tx
        self.__blk = blk
        self.__layout = layout
        self.__tx.pin(self.__blk)  # Pin the block in the buffer pool

    def set_int(self, slot: int, field_name: str, value: int):
        """
        Set an integer value for a specified field in a slot.

        Args:
            slot (int): The slot number where the record is stored.
            field_name (str): The name of the field to set.
            value (int): The integer value to set.

        Raises:
            KeyError: If the field name does not exist in the schema.
        """
        field_pos = self.__get_field_pos(slot, field_name)
        self.__tx.set_int(self.__blk, field_pos, value, True)

    def get_int(self, slot: int, field_name: str) -> int:
        """
        Retrieve an integer value from a specified field in a slot.

        Args:
            slot (int): The slot number where the record is stored.
            field_name (str): The name of the field to retrieve.

        Returns:
            int: The integer value of the specified field.

        Raises:
            KeyError: If the field name does not exist in the schema.
        """
        field_pos = self.__get_field_pos(slot, field_name)
        return self.__tx.get_int(self.__blk, field_pos)

    def set_string(self, slot: int, field_name: str, value: str):
        """
        Set a string value for a specified field in a slot.

        Args:
            slot (int): The slot number where the record is stored.
            field_name (str): The name of the field to set.
            value (str): The string value to set.

        Raises:
            KeyError: If the field name does not exist in the schema.
        """
        field_pos = self.__get_field_pos(slot, field_name)
        self.__tx.set_string(self.__blk, field_pos, value, True)

    def get_string(self, slot: int, field_name: str) -> str:
        """
        Retrieve a string value from a specified field in a slot.

        Args:
            slot (int): The slot number where the record is stored.
            field_name (str): The name of the field to retrieve.

        Returns:
            str: The string value of the specified field.

        Raises:
            KeyError: If the field name does not exist in the schema.
        """
        field_pos = self.__get_field_pos(slot, field_name)
        return self.__tx.get_string(self.__blk, field_pos)

    def delete(self, slot: int):
        """
        Delete a record by marking its slot as empty.

        Args:
            slot (int): The slot number where the record is stored.
        """
        self.__set_flag(slot, self.EMPTY)

    def format(self):
        """
        Format the block by setting all slots to empty and initializing fields.

        This method iterates through all slots in the block, marks them as empty,
        and initializes each field to its default value (0 for INT and empty string for STRING).
        """
        slot = 0
        while self.__is_valid_slot(slot):
            self.__set_flag(slot, self.EMPTY)
            for field_name in self.__layout.schema.fields:
                field_pos = self.__get_field_pos(slot, field_name)
                field_type = self.__layout.schema.get_field_type(field_name)
                if field_type == FieldType.INT:
                    self.__tx.set_int(self.__blk, field_pos, 0, False)
                elif field_type == FieldType.STRING:
                    self.__tx.set_string(self.__blk, field_pos, "", False)
                else:
                    raise ValueError(f"Unsupported FieldType '{field_type}' for field '{field_name}'.")
            slot += 1

    def next_after(self, slot: int) -> int:
        """
        Find the next used slot after the given slot.

        Args:
            slot (int): The current slot number.

        Returns:
            int: The next used slot number, or -1 if no such slot exists.
        """
        # print(f"In next_after, slot: {slot}")
        return self.__search_after(slot, self.USED)

    def insert_after(self, slot: int) -> int:
        """
        Insert a new record after the given slot.

        Finds the next available (empty) slot after the current slot.
        If no empty slot is found, returns -1.

        Args:
            slot (int): The current slot number.

        Returns:
            int: The new slot number where the record was inserted, or -1 if no slot is available.
        """
        new_slot = self.__search_after(slot, self.EMPTY)
        if new_slot != -1:
            self.__set_flag(new_slot, self.USED)
        return new_slot

    @property
    def block(self) -> BlockID:
        """
        Get the BlockID associated with this RecordPage.

        Returns:
            BlockID: The block identifier.
        """
        return self.__blk

    def __get_field_pos(self, slot: int, field_name: str) -> int:
        """
        Calculate the byte position of a field within a slot.

        Args:
            slot (int): The slot number.
            field_name (str): The name of the field.

        Returns:
            int: The byte position of the field within the block.

        Raises:
            KeyError: If the field name does not exist in the schema.
        """
        slot_offset = self.__offset(slot)
        field_offset = self.__layout.get_offset(field_name)
        return slot_offset + field_offset

    def __set_flag(self, slot: int, flag: int):
        """
        Set the usage flag for a specified slot.

        Args:
            slot (int): The slot number.
            flag (int): The flag to set (EMPTY or USED).

        Raises:
            ValueError: If the flag is not recognized.
        """
        if flag not in (self.EMPTY, self.USED):
            raise ValueError("Flag must be EMPTY (0) or USED (1).")
        self.__tx.set_int(self.__blk, self.__offset(slot), flag, True)

    def __search_after(self, slot: int, flag: int) -> int:
        """
        Search for the next slot with a specific flag after the given slot.

        Args:
            slot (int): The current slot number.
            flag (int): The flag to search for (EMPTY or USED).

        Returns:
            int: The slot number that matches the flag, or -1 if no such slot exists.
        """
        slot += 1
        while self.__is_valid_slot(slot):
            # print(f"Try get int blk: {str(self.__blk)}, slot: {slot}")
            current_flag = self.__tx.get_int(self.__blk, self.__offset(slot))
            if current_flag == flag:
                return slot
            slot += 1
        return -1

    def __is_valid_slot(self, slot: int) -> bool:
        """
        Check if the slot number is within the valid range of the block.

        Args:
            slot (int): The slot number to check.

        Returns:
            bool: True if the slot is valid, False otherwise.
        """
        slot_end_offset = self.__offset(slot + 1)
        return slot_end_offset <= self.__tx.block_size

    def __offset(self, slot: int) -> int:
        """
        Calculate the byte offset of a slot within the block.

        Args:
            slot (int): The slot number.

        Returns:
            int: The byte offset of the slot.
        """
        return slot * self.__layout.slot_size
