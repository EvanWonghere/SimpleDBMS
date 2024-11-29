# -*- coding: utf-8 -*-
# @Time    : 2024/11/28 19:26
# @Author  : EvanWong
# @File    : ConcurrencyMgr.py
# @Project : TestDB
import time
from file.BlockID import BlockID
from tx.concurrency.LockTable import LockTable


class ConcurrencyMgr:
    """Concurrency manager for transactions.

    This class manages locks for individual transactions and interacts with a global lock table.
    Each transaction has its own concurrency manager that keeps track of locks held by that transaction.

    Attributes:
        __lock_table (LockTable): Global lock table that coordinates locks across all transactions.
        __locks (dict): A dictionary mapping BlockIDs to lock types ("S" or "X") for this transaction.
    """

    # The global lock table, shared across all instances
    __lock_table: LockTable = LockTable()

    def __init__(self):
        """Initialize the ConcurrencyMgr instance for a transaction."""
        self.__locks: dict[BlockID, str] = dict()  # Dictionary to track locks held by this transaction

    def s_lock(self, blk: BlockID) -> bool:
        """
        Attempt to acquire a shared (S) lock on the given block.

        If the block is not already locked by the current transaction, attempt to acquire the S lock
        from the global lock table.

        Args:
            blk (BlockID): The block to acquire the lock for.

        Returns:
            bool: True if the lock was successfully acquired, False if unable to acquire the lock.
        """
        if blk not in list(self.__locks.keys()):  # If the block is not already locked by this transaction
            if not self.__lock_table.s_lock(blk):  # Attempt to acquire the S lock from the global lock table
                return False  # Lock acquisition failed
            self.__locks[blk] = "S"  # Mark the block as locked with an S lock
            return True
        return True  # Block is already locked by this transaction, can't acquire again

    def x_lock(self, blk: BlockID) -> bool:
        """
        Attempt to acquire an exclusive (X) lock on the given block.

        If the block already has a shared (S) lock, try to upgrade it to an exclusive lock.
        If the block is not locked by the current transaction, acquire an S lock first and then upgrade to X.

        Args:
            blk (BlockID): The block to acquire the lock for.

        Returns:
            bool: True if the lock was successfully acquired, False if unable to acquire the lock.
        """
        if blk not in list(self.__locks.keys()) or self.__locks[blk] != "X":  # If not already exclusively locked by this transaction
            # If the block is locked with an S lock, try upgrading to X lock
            if blk in list(self.__locks.keys()) and self.__locks[blk] == "S":
                if not self.__lock_table.x_lock(blk):  # Try upgrading to X lock
                    return False  # Lock upgrade failed
                self.__locks[blk] = "X"  # Successfully upgraded to X lock
                return True

            # If the block is not locked by the current transaction, acquire S lock first
            if not self.s_lock(blk):
                return False  # If unable to acquire S lock, return False
            if not self.__lock_table.x_lock(blk):  # Try acquiring X lock after obtaining S lock
                self.__lock_table.unlock(blk)  # If X lock acquisition fails, release the S lock
                self.__locks.pop(blk, None)
                return False  # X lock acquisition failed
            self.__locks[blk] = "X"  # Successfully upgraded to X lock
            return True
        return True  # Already have an exclusive lock

    def release(self):
        """
        Release all locks held by this transaction.

        This method will unlock all blocks that the transaction holds locks on,
        and clear the internal lock tracking.
        """
        for blk in list(self.__locks.keys()):
            self.__lock_table.unlock(blk)  # Release each lock from the global lock table
        self.__locks.clear()  # Clear the internal lock tracking
