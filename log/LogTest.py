# -*- coding: utf-8 -*-
# @Time    : 2024/11/28 13:07
# @Author  : EvanWong
# @File    : LogTest.py
# @Project : TestDB

from LogMgr import LogMgr
from file.FileMgr import FileMgr
from file.Page import Page


def print_log_records(log_manager: LogMgr, message: str):
    """
    Print all log records from the log file.

    Args:
        log_manager (LogMgr): The log manager responsible for managing log records.
        message (str): A message to display before printing the log records.
    """
    print(message)
    iterator = log_manager.iterator()
    if not iterator.has_next():
        print("No records in the log file.")
        return

    while iterator.has_next():
        record = iterator.next()
        page = Page(bytearray(record))
        string_value = page.get_string(0)
        int_position = Page.max_length(len(string_value))
        int_value = page.get_int(int_position)
        print(f"[{string_value}, {int_value}]")


def create_log_record(string_value: str, int_value: int) -> bytearray:
    """
    Create a log record containing a string and an integer.

    Args:
        string_value (str): The string part of the log record.
        int_value (int): The integer part of the log record.

    Returns:
        bytearray: The serialized log record.
    """
    start_position = 0
    string_position = start_position + Page.max_length(len(string_value))
    total_size = string_position + 4  # Integer size is 4 bytes
    buffer = bytearray(total_size)
    page = Page(buffer)
    page.set_string(start_position, string_value)
    page.set_int(string_position, int_value)
    return bytearray(page.content)


def initialize_log_manager(directory: str, block_size: int, logfile: str) -> LogMgr:
    """
    Initialize the FileMgr and LogMgr instances.

    Args:
        directory (str): The directory to store the log files.
        block_size (int): The size of each block in bytes.
        logfile (str): The name of the log file.

    Returns:
        LogMgr: An instance of LogMgr.
    """
    file_manager = FileMgr(directory, block_size)
    log_manager = LogMgr(file_manager, logfile)
    return log_manager


if __name__ == "__main__":
    # Initialize the log manager
    log_directory = "logtest"
    block_size = 400
    log_file = "simpledb.log"

    log_manager = initialize_log_manager(log_directory, block_size, log_file)

    # Print initial log records
    print_log_records(log_manager, "The initial empty log file:")

    # Create and append log records
    print("Creating records:")
    for i in range(1, 36):
        record_string = f"record{i}"
        log_record = create_log_record(record_string, 100 + i)
        lsn = log_manager.append(log_record)
        print(f"LSN: {lsn}", end=" ")

    print("\n")

    # Print all log records after appending
    print_log_records(log_manager, "The log file now has these records:")
