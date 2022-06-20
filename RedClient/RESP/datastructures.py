

class RESPLike:
    '''
    Base class for all RESP data types.
    '''
    pass


class SimpleString(RESPLike):
    '''
    Simple String RESP data type.
    '''

    def __init__(self, val: str | bytes) -> None:
        '''
        New RESP Simple String (ascii).

        args:
        `val`: should be of type `str` or `bytes`.

        exceptions:
        `ValueError`: `val` not of type `str` nor `bytes`.
        `UnicodeDecodeError`: `val` not a valid ascii string.
        '''

        if isinstance(val, bytes):
            val = val.decode('ascii')

        if not isinstance(val, str):   
            raise ValueError("Cannot convert " + type(val).__name__ + " to SimpleString object.")

        self._value = val
        return


    def __str__(self) -> str:
        return self._value


    def __repr__(self) -> str:
        return "SimpleString(\"" + self._value + "\")"

    
    def serialize(self) -> bytes:
        '''
        Serialize the Simple String into a bytes object.

        return:
        `bytes`: binary encoded value of the string with a leading `+` byte and `\\r\\n` at the end.
        '''

        return b"+" + self._value.encode() + b"\r\n"


    def value(self) -> str:
        '''
        Return the string stored in SimpleString.
        '''
        return self._value




class Integer(RESPLike, int):
    '''
    Integer RESP data type.
    '''

    def __init__(self, val: int =0) -> None:
        '''
        New RESP Integer.

        args:
        `val`: Integer value. the object type should support int conversion.

        exceptions:
        `ValueError`: `val` argument cannot be converted to `int`.
        '''

        self._value = int(val)
        super().__init__()


    def __str__(self) -> str:
        return str(self._value)
    

    def __repr__(self) -> str:
        return "Integer(" + str(self._value) + ")"


    def __int__(self) -> int:
        return self._value

    
    def serialize(self) -> bytes:
        '''
        Serialize the Integer into a bytes object.

        return:
        `bytes`: binary encoded value of the Integer with a leading `:` byte and `\\r\\n` at the end.
        '''
        return b":" + str(self._value).encode() + b"\r\n"




class Array(RESPLike, list):
    '''
    Array RESP data type.
    '''

    def __init__(self, items: list[RESPLike] =[]) -> None:
        '''
        New RESP Array.

        args:
        `items`: list of items to initialize the array with. should be one of RESP data types.
        
        exceptions:
        `ValueError`: `items` should be of a `list` and all objects in the list should be RESP object.
        '''

        if not isinstance(items, list):
            raise ValueError("Array items arguemnt should be of type list. got " + type(items).__name__)
        
        self._items = []

        for item in items:
            if not isinstance(item, RESPLike):
                raise ValueError("Expected a RESP object, got " + type(item).__name__)
            self.append(item)

        super().__init__()


    def append(self, item: RESPLike) -> None:
        '''
        Append item to the end of the array.

        args:
        `item`: object to append (should be a RESP object).

        exceptions:
        `ValueError`: `item` should be a RESP object.
        '''

        if not isinstance(item, RESPLike):
            raise ValueError("Cannot append object of type " + type(item).__name__ + " to array")
        
        self._items.append(item)

        return


    def __str__(self) -> str:
        s = ""
        for item in self._items:
            s += str(item) + ", "

        return "[" + s[:-2] + "]"


    def __repr__(self) -> str:
        r = ""
        for item in self._items:
            r += repr(item) + ", "

        return "Array([" + r[:-2] + "])"


    def __len__(self) -> int:
        return len(self._items)


    def __getitem__(self, key: int) -> RESPLike:
        return self._items[key]


    def __setitem__(self, key: int, value: RESPLike) -> None:

        if not isinstance(value, RESPLike):
            raise ValueError("Cannot set array item of type " + type(value).__name__)
        
        self._items[key] = value

        return


    def serialize(self) -> bytes:
        '''
        Serialize the Array into a bytes object.
        First the array header is constructed of `*` byte followed by the array length as a binary encoded string then followed by `\\r\\n`.
        Then, for every element in the array the returned value of the `serialize()` method of that element is appended to the bytes object.

        return:
        `bytes`: binary encoded value of the Array.
        '''

        b = b"*" + str(len(self._items)).encode() + b"\r\n"

        for item in self._items:
            b += item.serialize()

        return b




class BulkString(RESPLike):
    '''
    Bulk String RESP data type.
    '''

    def __init__(self, val: bytes | str) -> None:
        '''
        New RESP Bulk String.
        Unlike SimpleString, BulkString stores the value in bytes format.

        args:
        `val`: bytes value, should be either of type `str` or `bytes`

        exceptions:
        `ValueError`: `val` should be of type `str` or `bytes`.
        '''

        if isinstance(val, str):
            val = val.encode()

        if not isinstance(val, bytes):
            raise ValueError("Cannot convert " + type(val).__name__ + " to BulkString object.")

        self._value = val
        return


    def __str__(self) -> str:
        return str(self._value)


    def __repr__(self) -> str:
        return "BulkString(" + str(self._value) + ")"

    
    def serialize(self) -> bytes:
        '''
        Serialize the Bulk String into a bytes object.

        return:
        `bytes`: binary encoded value of the string with a leading `$` byte and `\\r\\n` at the end.
        '''

        return b"$" + str(len(self._value)).encode() + b"\r\n" + self._value + b"\r\n"


    def value(self) -> bytes:
        '''
        Get value of the Bulk String.

        return:
        `bytes`: bytes value of the string.
        '''
        return self._value




class Error(RESPLike):
    '''
    Error RESP data type.
    '''

    def __init__(self, msg: str|bytes ="") -> None:
        '''
        New RESP Error.

        args:
        `msg`: Error message, should be either of type `str` or `bytes`.

        exceptions:
        `ValueError`: args should be of type `str` or `bytes`.
        `UnicodeDecodeError`: could not decode the bytes into a valid ascii string.
        '''

        if isinstance(msg, bytes):
            msg = msg.decode('ascii')

        if not isinstance(msg, str):
            raise ValueError("Expected error message type str or bytes, got " + type(msg).__name__)

        self._message = msg
        return


    def __str__(self) -> str:
        return self._message


    def __repr__(self) -> str:
        return f'Error("{self._message}")'

    
    def serialize(self) -> bytes:
        '''
        Serialize the Error into a bytes object.

        return:
        `bytes`: binary encoded value of the Error with a leading `-` byte and `\\r\\n` at the end.
        '''
        return b"-" + self._message.encode() + b"\r\n"


    def message(self) -> str:
        '''
        Get the error message.

        return:
        `str`: error message.
        '''
        return self._message




class Null(RESPLike):
    '''
    NULL RESP data type.
    '''
    def __init__(self) -> None:
        '''
        New NULL RESP object.
        '''
        return

    def __str__(self) -> str:
        return "NULL"

    def __repr__(self) -> str:
        return "Null()"

    def serialize(self) -> bytes:
        '''
        Serialize the NULL object into a bytes object.

        return:
        `bytes`: binary encoded value of the string `$-1\\r\\n` (bulkString with size -1).
        '''

        return b"$-1\r\n"