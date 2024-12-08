# -*- coding: utf-8 -*-
# @Time    : 2024/9/12 20:20
# @Author  : EvanWong
# @File    : Page.py
# @Project : TestDB

import struct
import typing
import datetime

class Page:
    """Represents a data page in memory.

    A page is a buffer of data used to read/write blocks from/to disk. It supports various data types
    like integers, bytes, strings, floats, and dates.

    Attributes:
        __CHARSET (str): The character encoding used for strings. Defaults to 'utf-8'.
        __bb (bytearray): The internal buffer storing the page data.

    """

    __CHARSET = 'utf-8'

    def __init__(self, b: typing.Union[int, bytearray]):
        """
        Initializes a Page with either a size (int) for an empty buffer or a bytearray for an existing buffer.

        Args:
            b (Union[int, bytearray]): If int, initializes an empty buffer of that size. If bytearray, initializes the buffer with the given data.
        """
        self.__bb = bytearray(b)

    def get_int(self, offset: int) -> int:
        """Reads an integer from the buffer at the specified offset.

        Args:
            offset (int): The offset within the buffer to start reading.

        Returns:
            int: The integer value at the specified offset.
        """
        return struct.unpack_from('!i', self.__bb, offset)[0]

    def set_int(self, offset: int, num: int):
        """Writes an integer to the buffer at the specified offset.

        Args:
            offset (int): The offset within the buffer to start writing.
            num (int): The integer value to be written.
        """
        struct.pack_into('!i', self.__bb, offset, num)

    def get_bytes(self, offset: int) -> bytearray:
        """Reads bytes from the buffer at the specified offset.

        Args:
            offset (int): The offset within the buffer to start reading.

        Returns:
            bytes: The bytes read from the buffer.
        """
        length = self.get_int(offset)
        start_position = offset + 4  # 4 is the length of the stored integer
        return bytearray(self.__bb[start_position: start_position + length])

    def set_bytes(self, offset: int, b: bytearray):
        """Writes bytes to the buffer at the specified offset.

        Args:
            offset (int): The offset within the buffer to start writing.
            b (bytes): The bytes to be written to the buffer.
        """
        length = len(b)
        start_position = offset + 4  # 4 is the length of the stored integer

        self.set_int(offset, length)
        self.__bb[start_position: start_position + length] = b

    def get_string(self, offset: int) -> str:
        """Reads a string from the buffer at the specified offset.

        Args:
            offset (int): The offset within the buffer to start reading.

        Returns:
            str: The string read from the buffer.
        """
        b = self.get_bytes(offset)
        try:
            string = b.decode(self.__CHARSET)
            return string
        except UnicodeDecodeError:
            return None

    def set_string(self, offset: int, s: str):
        """Writes a string to the buffer at the specified offset.

        Args:
            offset (int): The offset within the buffer to start writing.
            s (str): The string to be written.
        """
        b = s.encode(self.__CHARSET)
        self.set_bytes(offset, b)

    def set_float(self, offset: int, value: float):
        """Stores a floating-point number in the buffer at the specified offset.

        Args:
            offset (int): The offset within the buffer to start writing.
            value (float): The floating-point number to be written.
        """
        struct.pack_into('!f', self.__bb, offset, value)

    def get_float(self, offset: int) -> float:
        """Reads a floating-point number from the buffer at the specified offset.

        Args:
            offset (int): The offset within the buffer to start reading.

        Returns:
            float: The floating-point number read from the buffer.
        """
        return struct.unpack_from('!f', self.__bb, offset)[0]

    def set_date(self, offset: int, date: datetime.date):
        """Stores a date in the buffer as an integer (seconds since Unix epoch).

        Args:
            offset (int): The offset within the buffer to start writing.
            date (datetime.date): The date to be written.
        """
        timestamp = int((date - datetime.date(1970, 1, 1)).total_seconds())
        struct.pack_into('!i', self.__bb, offset, timestamp)

    def get_date(self, offset: int) -> datetime.date:
        """Reads a date from the buffer.

        Args:
            offset (int): The offset within the buffer to start reading.

        Returns:
            datetime.date: The date read from the buffer.
        """
        timestamp = struct.unpack_from('!i', self.__bb, offset)[0]
        return datetime.date(1970, 1, 1) + datetime.timedelta(seconds=timestamp)

    @staticmethod
    def max_length(strlen: int) -> int:
        """Calculates the maximum number of bytes required to store a string of the given length.

        Args:
            strlen (int): The length of the string.

        Returns:
            int: The maximum number of bytes required.
        """
        bytes_per_char = len('A'.encode(Page.__CHARSET))
        return 4 + strlen * bytes_per_char

    @property
    def content(self) -> bytearray:
        """Returns the content of the page (buffer).

        Returns:
            bytearray: The buffer containing the page's data.
        """
        return self.__bb

    def write_content(self, data: bytearray):
        """
        Writes raw data directly to the internal buffer.

        Args:
            data (bytearray): The data to write. Its length must not exceed the buffer size.

        Raises:
            ValueError: If the data length exceeds the buffer size.
        """
        if len(data) > len(self.__bb):
            raise ValueError("data length exceeds the buffer size.")
        self.__bb[:len(data)] = data