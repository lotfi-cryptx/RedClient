import asyncio
import logging


class RESPType:
    '''
    Base class for all RESP data types.
    '''
    pass



class SimpleString(RESPType):
    '''
    Simple String RESP data type.
    '''

    def __init__(self, val) -> None:
        '''
        New RESP Simple String.

        args:
        `val`: String value, should be either of type `str` or `bytes`

        exceptions:
        `ValueError`: `val` should be of type `str` or `bytes`.
        `UnicodeDecodeError`: `val` is of type bytes, but could not decode the bytes into a valid ascii string.
        '''

        if isinstance(val, bytes):
            self._value = val.decode()
        elif isinstance(val, str):
            self._value = val
        else:
            raise ValueError("Cannot convert " + type(val).__name__ + " to SimpleString object.")

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
        Get value of the Simple String.

        return:
        `str`: string value.
        '''
        return self._value




class Integer(RESPType, int):
    '''
    Integer RESP data type.
    '''

    def __init__(self, val=0) -> None:
        '''
        New RESP Integer.

        args:
        `val`: Integer value. the object type should support int conversion.

        exceptions:
        `ValueError`: `val` argument cannot be converted into `int`.
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




class Array(RESPType, list):
    '''
    Array RESP data type.
    '''

    def __init__(self, items=[]) -> None:
        '''
        New RESP Array.

        args:
        `items`: list of items to initialize the array with. should be one of RESP data types.
        
        exceptions:
        `ValueError`: arg `items` should be of type `list` and every object in the list should be one of RESP data types.
        '''

        if not isinstance(items, list):
            raise ValueError("Array items arguemnt should be of type list. got " + type(items).__name__)
        
        self._items = []

        for item in items:
            self.append(item)

        super().__init__()


    def append(self, item: RESPType) -> None:
        '''
        Append item to the end of the array.

        args:
        `item`: the object that will be appended to the array.

        exceptions:
        `ValueError`: arg `item` should be one of RESP data types.

        return: None.
        '''

        if not isinstance(item, RESPType):
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


    def __getitem__(self, key: int) -> RESPType:
        return self._items[key]


    def __setitem__(self, key: int, value: RESPType) -> None:

        if not isinstance(value, RESPType):
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




class BulkString(RESPType):
    '''
    Bulk String RESP data type.
    '''

    def __init__(self, val) -> None:
        '''
        New RESP Bulk String.
        Unlike SimpleString, BulkString stores the value in bytes format.

        args:
        `val`: bytes value, should be either of type `str` or `bytes`

        exceptions:
        `ValueError`: `val` should be of type `str` or `bytes`.
        '''

        if isinstance(val, bytes):
            self._value = val
        elif isinstance(val, str):
            self._value = val.encode()
        else:
            raise ValueError("Cannot convert " + type(val).__name__ + " to BulkString object.")

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




class Error(RESPType):
    '''
    Error RESP data type.
    '''

    def __init__(self, err_prefix="ERR", err_message="") -> None:
        '''
        New RESP Error.

        args:
        `err_prefix`: Error type, should be either of type `str` or `bytes`.
        `err_message`: Error message, should be either of type `str` or `bytes`.

        exceptions:
        `ValueError`: args should be of type `str` or `bytes`.
        `UnicodeDecodeError`: could not decode the bytes into a valid ascii string.
        '''

        if isinstance(err_prefix, bytes):
            self._prefix = err_prefix.decode()
        elif isinstance(err_prefix, str):
            self._prefix = err_prefix
        else:
            raise ValueError("Expected error prefix type of str or bytes, got " + type(err_prefix).__name__)

        if isinstance(err_message, bytes):
            self._message = err_message.decode()
        elif isinstance(err_prefix, str):
            self._message = err_message
        else:
            raise ValueError("Expected error message type of str or bytes, got " + type(err_message).__name__)

        return


    def __str__(self) -> str:
        return self._prefix + ": (" + self._message + ")"


    def __repr__(self) -> str:
        return f'Error("{self._prefix}", "{self._message}")'

    
    def serialize(self) -> bytes:
        '''
        Serialize the Error into a bytes object.

        return:
        `bytes`: binary encoded value of the Error with a leading `-` byte and `\\r\\n` at the end.
        '''
        return b"-" + self._prefix.encode() + b" " + self._message.encode() + b"\r\n"


    def prefix(self) -> str:
        '''
        Get the error pefix (type).

        return:
        `str`: error prefix.
        '''
        return self._prefix

    def message(self) -> str:
        '''
        Get the error message.

        return:
        `str`: error message.
        '''
        return self._message




class Null(RESPType):
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




class Connection:
    '''
    RESP Connection class.
    Provides API to send and receive RESP data objects through the connection.
    Can be used to connect to a TCP server or use existing stream connection to use it in server implementation.
    '''

    class Error(Exception):
        '''
        Base exception for other exceptions.
        '''

    class Disconnected(Error):
        '''
        Raised when trying to send or receive data while TCP connection closed.
        '''
        pass

    class ParsingError(Error):
        '''
        Raised when an error occurs when parsing received data.
        '''
        pass

    class TimeoutError(Error):
        '''
        Raised when an operation times out.
        '''
        pass

    class ValueError(Error):
        '''
        Raised when an unexpected value is passed.
        '''
        pass

    class ConnectionRefusedError(Error):
        '''
        Raised when TCP connection refused to open.
        '''
        pass


    def __init__(self) -> None:
        '''
        New RESP Connection.
        '''
        self.reader=None
        self.writer=None
        return


    async def connect(self, host="127.0.0.1", port=6379):
        '''
        Create new TCP connection with remote host.

        args:
        `host`: host name or IP.
        `port`: port number.

        exceptions:
        `ConnectionRefusedError`: connection to the specified host refused.
        
        return:
        `Connection`: returns same connection object `self`.
        '''
        try:
            self.reader, self.writer = await asyncio.open_connection(host=host, port=port)
        except ConnectionRefusedError:
            raise Connection.ConnectionRefusedError

        return self


    def setStream(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        '''
        Set existing Stream connection as TCP connection.

        args:
        `reader`: Stream reader object to use.
        `Writer`: Stream writer object to use.

        exception:
        `ValueError`: invalid args data types.

        return:
        `Connection`: returns same connection object `self`.
        '''

        if not isinstance(reader, asyncio.StreamReader) or not isinstance(writer, asyncio.StreamWriter):
            raise Connection.ValueError(f"expected (StreamReader, StreamWriter) got ({type(reader).__name__}, {type(writer).__name__})")

        self.reader = reader
        self.writer = writer

        return self



    async def send(self, obj: Array) -> None:
        '''
        Serialize ``obj`` and send it through connection.
        Waits until data is transmitted. 

        args:
        ``obj``: RESP object to transmit.

        exceptions:
        ``ValueError``: obj argument not a RESP data type.
        ``Disconnected``: connection closed.

        return: None.
        '''

        if not isinstance(obj, RESPType):
            raise Connection.ValueError("obj should be one of RESP data types.")

        if not self.connected():
            await self.disconnect()
            raise Connection.Disconnected

        self.writer.write(obj.serialize())
        await self.writer.drain()

        return



    async def receive(self, timeout: int =None) -> RESPType:
        '''
        Wait for one RESP object and parse it.

        args:
        `timeout`: timeout period in seconds. if None, no timeout is imposed.

        exceptions:
        `ParsingError`: Unexpected value received.
        `Disconnected`: Connection closed before a valid RESP object was read.
        `TimeoutError`: Waiting for data timed out.

        return:
        `RESPType`: Received RESP object.
        '''

        try:

            if timeout:
                try:
                    line = await asyncio.wait_for(self.reader.readuntil(b'\n'), timeout)
                except asyncio.TimeoutError:
                    raise Connection.TimeoutError

            else:
                line = await self.reader.readuntil(b'\n')


            if line[-2] != ord('\r'):
                raise Connection.ParsingError("Line should end with \\r\\n")

            line = line[:-2]

            if len(line) > 0:


                if line[0] == ord('+'):
                    try:
                        return SimpleString(line[1:])
                    except UnicodeDecodeError:
                        raise Connection.ParsingError("Invalid SimpleString format")
                    

                if line[0] == ord('-'):

                    splitted = line[1:].split(b' ')

                    if len(splitted) > 1:
                        try:
                            return Error(splitted[0], b' '.join(splitted[1:]))
                        except UnicodeDecodeError:
                            raise Connection.ParsingError("Invalid Error format")

                    elif len(splitted) == 1:
                        try:
                            return Error(splitted[0], "")
                        except UnicodeDecodeError:
                            raise Connection.ParsingError("Invalid Error format")

                    else:
                        raise Connection.ParsingError("Invalid Error format")


                if line[0] == ord(':'):
                    try:
                        return Integer(line[1:])
                    except ValueError:
                        raise Connection.ParsingError("Invalid Integer format")


                if line[0] == ord('$'):
                    try:
                        stringLength = Integer(line[1:])
                    except ValueError:
                        raise Connection.ParsingError("Invalid BulkString length formar")

                    if stringLength == -1:
                        return Null()

                    if stringLength < -1:
                        raise Connection.ParsingError("Invalid BulkString length format")


                    if timeout:
                        try:
                            bulk = await asyncio.wait_for(self.reader.readexactly(stringLength + 2), timeout)
                        except asyncio.TimeoutError:
                            raise Connection.TimeoutError
                    else:
                        bulk = await self.reader.readexactly(stringLength + 2)


                    if bulk[-2] != ord('\r') or bulk[-1] != ord('\n'):
                        raise Connection.ParsingError("BulkString should end with \\r\\n")

                    return BulkString(bulk[:-2])


                if line[0] == ord('*'):
                    try:
                        arraySize = Integer(line[1:])
                    except ValueError:
                        raise Connection.ParsingError("Invalid Array size formar")

                    if arraySize == -1:
                        return Null()

                    if arraySize < -1:
                        raise Connection.ParsingError("Invalid Array size format")

                    array = Array()

                    for _ in range(arraySize):
                        arrayItem = await self.receive()
                        array.append(arrayItem)

                    return array

        except (asyncio.IncompleteReadError, asyncio.LimitOverrunError):
            await self.disconnect()
            raise Connection.Disconnected

        return



    def connected(self) -> bool:
        '''
        Check connection status.

        return:
        ``bool``: True if connected, else return False.

        Note: if connected() returns False, disconnect() should be called for cleanup.
        '''

        if not self.reader or not self.writer:
            return False

        if self.reader.at_eof():
            return False

        if self.writer.is_closing():
            return False

        return True



    async def disconnect(self) -> None:
        '''
        Close connection.

        return: None.
        '''

        if self.writer:
            self.writer.close()
            await self.writer.wait_closed()

        self.reader = None
        self.writer = None

        return





if __name__ == "__main__":

    async def client():

        try:
            client = await Connection().connect()
        
        except Connection.ConnectionRefusedError:
            logging.error("Connection to server refused.")
            exit(1)

        try:

            array = Array([BulkString("SUBSCRIBE"), BulkString("test1"), BulkString("test2")])

            await client.send(array)

            response = await client.receive()

            print(type(response).__name__ + " ==> " + str(response))

            response = await client.receive()

            print(type(response).__name__ + " ==> " + str(response))

            array = Array([BulkString("SUBSCRIBE"), BulkString("test3")])

            await client.send(array)

            response = await client.receive()

            print(type(response[2]))

            print(type(response).__name__ + " ==> " + str(response))

        except Exception as e:
            logging.error("Exception: " + str(e))

        finally:
            await client.disconnect()


    async def stream():

        try:
            reader, writer = await asyncio.open_connection(host="localhost", port=6379)
        
        except ConnectionRefusedError:
            logging.error("Connection to server refused.")
            exit(1)

        client = Connection().setStream(reader, writer)

        try:
            array = Array([BulkString("SET"), BulkString("test"), BulkString("123")])

            await client.send(array)

            response = await client.receive()

            print(type(response).__name__ + " ==> " + str(response))

        except Exception as e:
            logging.error("Exception: " + str(e))

        finally:
            await client.disconnect()
    
    asyncio.run(stream())
