# -*- coding: utf-8 -*-
# @Time    : 2024/11/28 19:21
# @Author  : EvanWong
# @File    : LockTable.py
# @Project : TestDB
import time
from file.BlockID import BlockID
from tx.concurrency.LockAbortException import LockAbortException


class LockTable:
    """
    LockTable is a simple implementation of a lock manager that handles
    shared (S) and exclusive (X) locks for disk blocks.

    The lock table stores the state of locks on each block:
        - An X lock is represented by the value -1
        - An S lock is represented by the value 1
        - A value of 0 indicates no lock is held on the block.

    Attributes:
        __MAX_TIME (int): Maximum time (in seconds) allowed for waiting for a lock.
        __locks (dict): Dictionary that maps BlockIDs to their lock values (int).

    Methods:
        s_lock(blk: BlockID): Acquire a shared lock (S lock) on the given block.
        x_lock(blk: BlockID): Acquire an exclusive lock (X lock) on the given block.
        unlock(blk: BlockID): Release the lock on the given block.
        __has_x_lock(blk: BlockID): Check if the block has an exclusive lock.
        __has_other_s_locks(blk: BlockID): Check if the block has other shared locks.
        __waiting_too_long(start_time: float): Check if the lock acquisition has timed out.
        __get_lock_value(blk: BlockID): Retrieve the current lock value for the block.
    """

    __MAX_TIME: int = 10  # Maximum wait time for acquiring a lock (in seconds)

    def __init__(self):
        """
        Initialize the lock table with an empty dictionary to track locks on blocks.
        """
        self.__locks: dict[BlockID, int] = {}

    def s_lock(self, blk: BlockID) -> bool:
        """
        Attempt to acquire a shared lock (S lock) on the given block.

        If the block already has an exclusive lock (X lock), this method will wait
        until the X lock is released or until the maximum waiting time is exceeded.

        Args:
            blk (BlockID): The block to acquire the lock on.

        Returns:
            bool: True if the lock was successfully acquired, False otherwise.
        """
        try:
            start_time = time.time()  # Record the time when we start trying to acquire the lock
            # Try to acquire the lock, waiting if an X lock is present
            while self.__has_x_lock(blk) and not self.__waiting_too_long(start_time):
                time.sleep(0.1)  # Sleep briefly to avoid busy-waiting
            if self.__has_x_lock(blk):  # If we still have an X lock after waiting, return False
                return False
            # No X lock, acquire the shared lock
            val = self.__get_lock_value(blk)
            self.__locks[blk] = val + 1  # Increment the lock count for the block
            return True
        except InterruptedError:
            return False  # Return False if interrupted

    def x_lock(self, blk: BlockID) -> bool:
        """
        Attempt to acquire an exclusive lock (X lock) on the given block.

        If the block already has shared locks, this method will wait until all S locks
        are released or until the maximum waiting time is exceeded.

        Args:
            blk (BlockID): The block to acquire the lock on.

        Returns:
            bool: True if the lock was successfully acquired, False otherwise.
        """
        try:
            start_time = time.time()  # Record the start time
            # Try to acquire the lock, waiting if shared locks are present
            while self.__has_other_s_locks(blk) and not self.__waiting_too_long(start_time):
                time.sleep(0.1)  # Sleep briefly to avoid busy-waiting
            if self.__has_other_s_locks(blk):  # If there are still shared locks, return False
                return False
            # No conflicting shared locks, acquire the exclusive lock
            self.__locks[blk] = -1  # Set the lock value to -1 for exclusive lock
            return True
        except InterruptedError:
            return False  # Return False if interrupted

    def unlock(self, blk: BlockID):
        """
        Release the lock on the given block.

        If the block has multiple shared locks, the lock count is decremented.
        If the block has no remaining locks, the lock entry is removed from the table.

        Args:
            blk (BlockID): The block to release the lock on.
        """
        val = self.__get_lock_value(blk)  # Retrieve the current lock value
        if val > 1:  # If there are multiple shared locks
            self.__locks[blk] = val - 1  # Decrement the lock count
        else:
            self.__locks.pop(blk, -1) # If there are no more locks, remove the block from the table

    def __has_x_lock(self, blk: BlockID) -> bool:
        """
        Check if the block has an exclusive lock.

        Args:
            blk (BlockID): The block to check for an exclusive lock.

        Returns:
            bool: True if the block has an exclusive lock, False otherwise.
        """
        return self.__get_lock_value(blk) < 0  # An exclusive lock is represented by a value of -1

    def __has_other_s_locks(self, blk: BlockID) -> bool:
        """
        Check if the block has any other shared lock.

        Args:
            blk (BlockID): The block to check for shared locks.

        Returns:
            bool: True if the block has one or more shared locks, False otherwise.
        """
        return self.__get_lock_value(blk) > 1  # Shared locks are represented by values greater than 1

    def __waiting_too_long(self, start_time: float) -> bool:
        """
        Check if the lock acquisition has timed out.

        Args:
            start_time (float): The time when the lock acquisition attempt started.

        Returns:
            bool: True if the waiting time has exceeded the maximum allowed, False otherwise.
        """
        return time.time() - start_time > self.__MAX_TIME

    def __get_lock_value(self, blk: BlockID) -> int:
        """
        Retrieve the current lock value for the given block.

        Args:
            blk (BlockID): The block to retrieve the lock value for.

        Returns:
            int: The current lock value for the block (0 for no lock, 1 for S lock, -1 for X lock).
        """
        val = self.__locks.get(blk)
        return 0 if val is None else val  # If the block has no lock, return 0
