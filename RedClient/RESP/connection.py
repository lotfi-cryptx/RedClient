import asyncio
from typing_extensions import Self

from .datastructures import RESPLike, BulkString, SimpleString, Integer, Array, Error, Null


class Connection:
    '''
    RESP Connection class.
    Provides API to send and receive RESP data objects through a TCP connection.
    Can be used to connect to a TCP server, or use existing stream connection to use it in server implementation.
    '''

    class Error(Exception):
        '''
        Base exception for other exceptions.
        '''

    class ConnectionClosedError(Error):
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


    async def connect(self, host="127.0.0.1", port=6379) -> Self:
        '''
        Create new TCP connection with remote host.

        args:
        `host`: hostname or IP.
        `port`: port number.

        exceptions:
        `ConnectionRefusedError`: connection to the specified host refused.
        
        return:
        `Connection`: returns this connection object `self`.
        '''
        try:
            self.reader, self.writer = await asyncio.open_connection(host=host, port=port)

        except ConnectionRefusedError:
            raise self.ConnectionRefusedError

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
        `Connection`: returns this connection object `self`.
        '''

        if not isinstance(reader, asyncio.StreamReader) or not isinstance(writer, asyncio.StreamWriter):
            raise self.ValueError(f"expected (StreamReader, StreamWriter) got ({type(reader).__name__}, {type(writer).__name__})")

        self.reader = reader
        self.writer = writer

        return self



    async def send(self, obj: RESPLike) -> None:
        '''
        Serialize `obj` and send it through connection.
        Waits until data is transmitted. 

        args:
        `obj`: RESPLike object.

        exceptions:
        `ValueError`: obj argument not a RESPLike.
        `ConnectionClosedError`: tcp connection closed.
        '''

        if not isinstance(obj, RESPLike):
            raise self.ValueError("obj should be one of RESP data types.")

        if not self.connected():
            await self.disconnect()
            raise self.ConnectionClosedError

        self.writer.write(obj.serialize())
        await self.writer.drain()

        return



    async def receive(self, timeout: int =None) -> RESPLike:
        '''
        Wait for one RESP object and parse it.

        args:
        `timeout`: timeout period in seconds. if None, no timeout is imposed.

        exceptions:
        `ParsingError`: Unexpected value received.
        `ConnectionClosedError`: Connection closed before a valid RESP object was read.
        `TimeoutError`: Waiting for data timed out.

        return:
        `RESPLike`: Received RESP object.
        '''

        try:

            if timeout:
                try:
                    line = await asyncio.wait_for(self.reader.readuntil(b'\n'), timeout)
                except asyncio.TimeoutError:
                    raise self.TimeoutError

            else:
                line = await self.reader.readuntil(b'\n')


            if line[-2] != ord('\r'):
                raise self.ParsingError("Line should end with \\r\\n")

            line = line[:-2]

            if len(line) > 0:

                if line[0] == ord('+'):
                    try:
                        return SimpleString(line[1:])

                    except UnicodeDecodeError:
                        raise self.ParsingError("Invalid SimpleString format")


                if line[0] == ord('-'):
                    try:
                        return Error(line[1:])

                    except UnicodeDecodeError:
                        raise self.ParsingError("Invalid Error format")


                if line[0] == ord(':'):
                    try:
                        return Integer(line[1:])

                    except ValueError:
                        raise self.ParsingError("Invalid Integer format")


                if line[0] == ord('$'):
                    try:
                        stringLength = Integer(line[1:])

                    except ValueError:
                        raise self.ParsingError("Invalid BulkString length formar")

                    if stringLength == -1:
                        return Null()

                    if stringLength < -1:
                        raise self.ParsingError("Invalid BulkString length format")


                    if timeout:
                        try:
                            bulk = await asyncio.wait_for(self.reader.readexactly(stringLength + 2), timeout)
                        except asyncio.TimeoutError:
                            raise self.TimeoutError
                    else:
                        bulk = await self.reader.readexactly(stringLength + 2)


                    if bulk[-2] != ord('\r') or bulk[-1] != ord('\n'):
                        raise self.ParsingError("BulkString should end with \\r\\n")

                    return BulkString(bulk[:-2])


                if line[0] == ord('*'):
                    try:
                        arraySize = Integer(line[1:])

                    except ValueError:
                        raise self.ParsingError("Invalid Array size format")

                    if arraySize == -1:
                        return Null()

                    if arraySize < -1:
                        raise self.ParsingError("Invalid Array size format")

                    array = Array()

                    for _ in range(arraySize):
                        arrayItem = await self.receive(timeout=timeout)
                        array.append(arrayItem)

                    return array

        except (asyncio.IncompleteReadError, asyncio.LimitOverrunError):
            await self.disconnect()
            raise self.ConnectionClosedError

        return



    def connected(self) -> bool:
        '''
        Check connection status.

        return:
        `bool`: True if connected, else return False.

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
        '''

        if self.writer:
            self.writer.close()
            await self.writer.wait_closed()

        self.reader = None
        self.writer = None

        return