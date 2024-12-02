# -*- coding: utf-8 -*-
# @Time    : 2024/11/30 11:40
# @Author  : EvanWong
# @File    : RID.py
# @Project : TestDB

class RID:
    """
    Represents a Record ID (RID) in the database.

    A RID uniquely identifies a record within a table by specifying the block number
    and the slot number within that block.

    Attributes:
        __blk_num (int): The block number where the record is stored.
        __slot (int): The slot number within the block where the record resides.
    """

    def __init__(self, blk_num: int, slot: int):
        """
        Initialize a Record ID with block number and slot number.

        Args:
            blk_num (int): The block number where the record is stored.
            slot (int): The slot number within the block where the record resides.

        Raises:
            ValueError: If either blk_num or slot is negative.
        """
        if blk_num < 0 or slot < 0:
            raise ValueError("Block number and slot number must be non-negative integers.")
        self.__blk_num: int = blk_num
        self.__slot: int = slot

    @property
    def block_number(self) -> int:
        """
        Get the block number of the record.

        Returns:
            int: The block number.
        """
        return self.__blk_num

    @property
    def slot(self) -> int:
        """
        Get the slot number of the record within its block.

        Returns:
            int: The slot number.
        """
        return self.__slot

    def __eq__(self, other) -> bool:
        """
        Check if this RID is equal to another RID.

        Args:
            other (RID): The other Record ID to compare against.

        Returns:
            bool: True if both RIDs have the same block and slot numbers, False otherwise.
        """
        if isinstance(other, RID):
            return self.block_number == other.block_number and self.slot == other.slot
        return NotImplemented

    def __hash__(self) -> int:
        """
        Compute the hash of the RID.

        This allows RIDs to be used in hashed collections like sets and dictionaries.

        Returns:
            int: The hash value of the RID.
        """
        return hash((self.__blk_num, self.__slot))

    def __str__(self) -> str:
        """
        Return the string representation of the RID.

        Returns:
            str: The RID in the format "[block_number, slot_number]".
        """
        return f"[{self.block_number}, {self.slot}]"
