# -*- coding: utf-8 -*-
# @Time    : 2024/12/2 15:20
# @Author  : EvanWong
# @File    : TableScan.py
# @Project : TestDB

from file.BlockID import BlockID
from query.Constant import Constant
from query.UpdateScan import UpdateScan
from record.FieldType import FieldType
from record.Layout import Layout
from record.RID import RID
from record.RecordPage import RecordPage
from tx.Transaction import Transaction


class TableScan(UpdateScan):
    """
    Implements a table scan that allows reading and updating records within a table.

    This class provides functionality to iterate through all records in a table,
    retrieve field values, and perform updates such as setting field values,
    inserting new records, and deleting existing records.
    """

    TABLE_FILE_SUFFIX = ".tbl"

    def __init__(self, tx: Transaction, table_name: str, layout: Layout):
        """
        Initialize a TableScan for a specific table.

        Args:
            tx (Transaction): The transaction managing this scan.
            table_name (str): The name of the table to scan.
            layout (Layout): The layout of the records in the table.
        """
        self.__tx: Transaction = tx
        self.__layout: Layout = layout
        self.__table_file_name: str = table_name + self.TABLE_FILE_SUFFIX

        self.__rp: RecordPage = None  # Current RecordPage
        self.__current_slot: int = None  # Current slot number

        # Initialize the scan by moving to the first block or creating a new block if table is empty
        if tx.size(self.__table_file_name) == 0:
            # print("Move to new block")
            self.__move_to_new_block()
        else:
            # print("Move to block zero")
            self.__move_to_block(0)

    def set_value(self, field_name: str, value: Constant):
        """
        Set the value of a specified field in the current record.

        Args:
            field_name (str): The name of the field to set.
            value (Constant): The new value to set for the field.

        Raises:
            KeyError: If the field name does not exist in the schema.
        """
        field_type = self.__layout.schema.get_field_type(field_name)
        if field_type == FieldType.INT:
            self.__rp.set_int(self.__current_slot, field_name, value.as_int())
        elif field_type == FieldType.STRING:
            self.__rp.set_string(self.__current_slot, field_name, value.as_str())
        else:
            raise ValueError(f"Unsupported FieldType '{field_type}' for field '{field_name}'.")

    def set_int(self, field_name: str, value: int):
        """
        Set an integer value for a specified field in the current record.

        Args:
            field_name (str): The name of the field to set.
            value (int): The integer value to set.

        Raises:
            KeyError: If the field name does not exist in the schema.
        """
        self.__rp.set_int(self.__current_slot, field_name, value)

    def set_string(self, field_name: str, value: str):
        """
        Set a string value for a specified field in the current record.

        Args:
            field_name (str): The name of the field to set.
            value (str): The string value to set.

        Raises:
            KeyError: If the field name does not exist in the schema.
        """
        self.__rp.set_string(self.__current_slot, field_name, value)

    def insert(self):
        """
        Insert a new record into the table.

        This method finds the next available (empty) slot to insert the new record.
        If no empty slot is found in the current block, it moves to the next block.
        If no more blocks exist, it appends a new block to the table.

        Raises:
            RuntimeError: If unable to insert a new record due to unexpected errors.
        """
        self.__current_slot = self.__rp.insert_after(self.__current_slot)
        while self.__current_slot == -1:
            if self.__at_last_block():
                self.__move_to_new_block()
            else:
                next_block_num = self.__rp.block.number + 1
                self.__move_to_block(next_block_num)
            self.__current_slot = self.__rp.insert_after(self.__current_slot)

    def delete(self):
        """
        Delete the current record from the table.

        This method marks the current slot as empty.
        """
        self.__rp.delete(self.__current_slot)

    def get_rid(self) -> RID:
        """
        Retrieve the Record ID (RID) of the current record.

        Returns:
            RID: The Record ID of the current record.
        """
        return RID(self.__rp.block.number, self.__current_slot)

    def move_to_rid(self, rid: RID):
        """
        Move the scan to a specific record identified by its RID.

        Args:
            rid (RID): The Record ID to move the scan to.
        """
        self.close()  # Unpin the current block
        blk = BlockID(self.__table_file_name, rid.block_number)
        self.__rp = RecordPage(self.__tx, blk, self.__layout)
        self.__current_slot = rid.slot

    def before_first(self):
        """
        Move the scan's position before the first block.

        This method resets the scan so that the next call to `next()` will position it at the first record.
        """
        self.__move_to_block(0)

    def next(self) -> bool:
        """
        Advance the scan to the next record.

        This method finds the next used slot and updates the current slot position.

        Returns:
            bool: True if a next record is found, False if the end of the table is reached.
        """
        # print(f"In TableScan.next(), current_slot is {self.__current_slot}")
        self.__current_slot = self.__rp.next_after(self.__current_slot)
        while self.__current_slot < 0:
            if self.__at_last_block():
                return False
            next_block_num = self.__rp.block.number + 1
            self.__move_to_block(next_block_num)
            self.__current_slot = self.__rp.next_after(self.__current_slot)
        return True

    def get_int(self, field_name: str) -> int:
        """
        Get an integer value from a specified field in the current record.

        Args:
            field_name (str): The name of the field to retrieve.

        Returns:
            int: The integer value of the specified field.

        Raises:
            KeyError: If the field name does not exist in the schema.
        """
        return self.__rp.get_int(self.__current_slot, field_name)

    def get_string(self, field_name: str) -> str:
        """
        Get a string value from a specified field in the current record.

        Args:
            field_name (str): The name of the field to retrieve.

        Returns:
            str: The string value of the specified field.

        Raises:
            KeyError: If the field name does not exist in the schema.
        """
        return self.__rp.get_string(self.__current_slot, field_name)

    def get_value(self, field_name: str) -> Constant:
        """
        Get the value of a specified field in the current record as a Constant.

        Args:
            field_name (str): The name of the field to retrieve.

        Returns:
            Constant: The value of the specified field encapsulated in a Constant object.

        Raises:
            KeyError: If the field name does not exist in the schema.
        """
        field_type = self.__layout.schema.get_field_type(field_name)
        if field_type == FieldType.INT:
            return Constant(self.get_int(field_name))
        elif field_type == FieldType.STRING:
            return Constant(self.get_string(field_name))
        else:
            raise ValueError(f"Unsupported FieldType '{field_type}' for field '{field_name}'.")

    def has_field(self, field_name: str) -> bool:
        """
        Check if the schema contains a specified field.

        Args:
            field_name (str): The name of the field to check.

        Returns:
            bool: True if the field exists in the schema, False otherwise.
        """
        return self.__layout.schema.has_field(field_name)

    def close(self):
        """
        Close the scan and release any associated resources.

        This method unpins the current block from the buffer pool.
        """
        if self.__rp is not None:
            # print("TableScan called unpin")
            self.__tx.unpin(self.__rp.block)
            self.__rp = None
            self.__current_slot = -1

    def __move_to_block(self, blk_num: int):
        """
        Move the scan to a specific block number.

        Args:
            blk_num (int): The block number to move to.
        """
        self.close()
        blk = BlockID(self.__table_file_name, blk_num)
        self.__rp = RecordPage(self.__tx, blk, self.__layout)
        self.__current_slot = -1  # Initialize to before the first slot

    def __move_to_new_block(self):
        """
        Append a new block to the table and initialize its slots.

        This method creates a new block, formats it, and sets the current slot to -1.
        """
        self.close()
        blk = self.__tx.append(self.__table_file_name)
        self.__rp = RecordPage(self.__tx, blk, self.__layout)
        self.__rp.format()  # Initialize all slots in the new block
        self.__current_slot = -1

    def __at_last_block(self) -> bool:
        """
        Check if the current block is the last block in the table.

        Returns:
            bool: True if the current block is the last block, False otherwise.
        """
        return self.__rp.block.number == self.__tx.size(self.__table_file_name) - 1

    @property
    def get_current_slot(self) -> int:
        return self.__current_slot
