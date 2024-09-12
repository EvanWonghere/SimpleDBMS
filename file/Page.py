# -*- coding: utf-8 -*-
# @Time    : 2024/9/12 20:20
# @Author  : EvanWong
# @File    : Page.py
# @Project : TestDB
import struct
import typing


class Page:
    """ The implementation of page.
    A page is a buffer of data.

    Attributes:
        __CHARSET: Character encoding method.
        __bb: Data buffer.
    """

    __CHARSET = 'utf-8'

    def __init__(self, b: typing.Union[int | bytearray]):
        """
        Constructors cannot be refactored in Python,
        so two constructors are implemented using two different numeric types,
        int and bytearray.

        Args:
            b: Used by the buffer manager when b is of type int
               and by the log manager when b is of type bytearray
        """
        self.__bb = bytearray(b)

    def get_int(self, offset: int) -> int:
        """
        Read an integer value from the given position.

        Args:
            offset: The offset corresponding to the specified position.

        Returns: The integer value read from the given position.
        """
        return struct.unpack_from('!i', self.__bb, offset)[0]

    def set_int(self, offset: int, num: int):
        """
        Write an integer value to the given position.

        Args:
            offset: The offset corresponding to the specified position.
            num: The integer value to be written.

        Returns: None
        """
        return struct.pack_into('!i', self.__bb, offset, num)

    def get_bytes(self, offset: int) -> bytes:
        """
        Read bytes from the given position.

        Args:
            offset: The relative offset of the storage position where the number of bytes to read is stored.

        Returns: The bytes read from the give position.
        """
        length: int = self.get_int(offset)
        start_position: int = offset + 4  # 4 is the length of the stored integer
        return bytes(self.__bb[start_position: start_position + length])

    def set_bytes(self, offset: int, b: bytes):
        """
        Write bytes to the given position.

        Args:
            offset: The relative offset of the storage position where the number of bytes to write is stored.
            b: The bytes to be written

        Returns: None
        """
        length: int = len(b)
        start_position: int = offset + 4  # 4 is the length of the stored integer

        self.set_int(offset, length)
        self.__bb[start_position: start_position + length] = b

    def get_string(self, offset: int) -> str:
        """
        Read string from the given position.

        Args:
            offset: The offset corresponding to the specified position.

        Returns: The string read from the given position.
        """
        b = self.get_bytes(offset)
        return b.decode(self.__CHARSET)

    def set_string(self, offset: int, s: str):
        """
        Read string from the given position.

        Args:
            offset: The offset corresponding to the specified position.
            s: The string to be written.

        Returns: None
        """
        b = s.encode(self.__CHARSET)
        self.set_bytes(offset, b)

    @staticmethod
    def max_length(strlen: int) -> int:
        """
        Computes the maximum number of bytes required to store a string of a specified length.

        Args:
            strlen: The length of the string.

        Returns: The maximum number of bytes required to store a string of a specified length.
        """
        bytes_per_char: int = len('A'.encode(Page.__CHARSET))
        return 4 + strlen * bytes_per_char

    @property
    def content(self) -> bytearray:
        return self.__bb
