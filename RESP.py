import asyncio



class RESPType:
    def __init__(self) -> None:
        pass


class SimpleString(RESPType):


    def __init__(self, val) -> None:

        if isinstance(val, bytes):
            self._value = val.decode()
        elif isinstance(val, str):
            self._value = val
        else:
            raise ValueError("Cannot convert " + type(val).__name__ + " to SimpleString object.")
        
        super().__init__()


    def __str__(self) -> str:
        return self._value

    def __repr__(self) -> str:
        return "SimpleString(\"" + self._value + "\")"

    
    def serialize(self) -> bytes:
        return b"+" + self._value.encode() + b"\r\n"

    def value(self) -> str:
        return self._value



class Integer(RESPType, int):


    def __init__(self, val=0) -> None:

        self._value = int(val)
        super().__init__()


    def __str__(self) -> str:
        return str(self._value)
    
    def __repr__(self) -> str:
        return "Integer(" + str(self._value) + ")"


    def __int__(self) -> int:
        return self._value

    
    def serialize(self) -> bytes:
        return b":" + str(self._value).encode() + b"\r\n"




class Array(RESPType, list):


    def __init__(self, items=[]) -> None:

        if not isinstance(items, list):
            raise ValueError("Array items arguemnt should be of type list. got " + type(items).__name__)
        
        self._items = []

        for item in items:
            self.append(item)

        super().__init__()


    def append(self, item: RESPType) -> None:
        '''
        Append item to the end of the array
        '''
        if not isinstance(item, RESPType):
            raise ValueError("Cannot append object of type " + type(item).__name__ + " to array")
        
        self._items.append(item)


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

    def __getitem__(self, key) -> RESPType:
        return self._items[key]

    def __setitem__(self, key, value) -> RESPType:

        if not isinstance(value, RESPType):
            raise ValueError("Cannot set array item of type " + type(value).__name__)
        
        self._items[key] = value


    def serialize(self) -> bytes:
        b = b"*" + str(len(self._items)).encode() + b"\r\n"

        for item in self._items:
            b += item.serialize()

        return b




class BulkString(SimpleString):

    def __init__(self, val="") -> None:
        super().__init__(val=val)

    def __repr__(self) -> str:
        return "BulkString(\"" + super().__str__() + "\")"

    def serialize(self) -> bytes:
        return b"$" + str(len(self.value())).encode() + b"\r\n" + self.value().encode() + b"\r\n"




class Error(SimpleString):

    def __init__(self, val="") -> None:
        super().__init__(val=val)

    def __repr__(self) -> str:
        return "Error(\"" + super().__str__() + "\")"

    def serialize(self) -> bytes:
        return b"-" + self.value().encode() + b"\r\n"




class Null(RESPType):

    def __init__(self) -> None:
        super().__init__()

    def __str__(self) -> str:
        return "NULL"

    def __repr__(self) -> str:
        return "Null()"




class ClientConnection:

    def __init__(self, host="127.0.0.1", port=6379) -> None:
        '''
        Initialize the RESP client
        '''
        self.host=host
        self.port=port
        self.reader=None
        self.writer=None
        
        return


    async def connect(self) -> None:
        '''
        Connect to redis server
        '''
        self.reader, self.writer = await asyncio.open_connection(host=self.host, port=self.port)

        return


    async def send(self, arr: Array) -> None:
        '''
        Send a request as a RESP Array of Bulk Strings
        '''

        if not self.connected():
            await self.disconnect()

        self.writer.write(arr.serialize())

        return


    async def receive(self) -> RESPType:
        '''
        Wait for a response from the server and parse it
        '''

        line = await self.reader.readuntil(b'\n')


        if line[-2] != ord('\r'):
            raise Exception("Line should end with \\r\\n")

        line = line[:-2]

        if len(line) > 0:
            if line[0] == ord('+'):
                return SimpleString(line[1:])
            
            if line[0] == ord('-'):
                return Error(line[1:])
            
            if line[0] == ord(':'):
                return Integer(line[1:])

            if line[0] == ord('$'):
                stringLength = Integer(line[1:])

                bulk = await self.reader.readexactly(stringLength + 2)

                if bulk[-2] != ord('\r') or bulk[-1] != ord('\n'):
                    raise Exception("Bulk string should end with \\r\\n")

                return BulkString(bulk[:-2])

            if line[0] == ord('*'):
                arraySize = Integer(line[1:])

                array = Array()

                for _ in range(arraySize):
                    arrayItem = await self.receive()
                    array.append(arrayItem)

                return array

        return


    def connected(self) -> bool:

        if not self.reader or not self.writer:
            return False

        if self.reader.at_eof():
            return False

        if self.writer.is_closing():
            return False
        
        return True


    async def disconnect(self) -> None:
        
        if self.writer:
            self.writer.close()
            await self.writer.wait_closed()

        self.reader = None
        self.writer = None

        return



async def main():

    client = ClientConnection()

    try:
        await client.connect()

        array = Array([BulkString("PUBLISH"), BulkString("test"), BulkString("smthng")])

        await client.send(arr=array)

        response = await client.receive()

        print(type(response).__name__ + " ==> " + str(response))

    finally:
        await client.disconnect()


asyncio.run(main())
