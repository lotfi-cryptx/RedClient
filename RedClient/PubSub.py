import asyncio

import RedClient.RESP as RESP



class Publisher():

    class Error(Exception):
        '''
        Base class for exceptions.
        '''
        pass

    class ConnectionRefused(Error):
        '''
        Raised when client can not connect to Redis server.
        '''
        pass

    class ValueError(Error):
        '''
        Raised when an unexpected value is passed.
        '''
        pass

    class TimeoutError(Error):
        '''
        Raised when an operation times out.
        '''
        pass

    class ClientDisconnected(Error):
        '''
        Raised when the client is not connected to the Redis server.
        '''
        pass

    class ParsingError(Error):
        '''
        Raised when an unexpected value is received from server.
        '''
        pass

    class ServerError(Error):
        '''
        Raised when the server sends a RESP error response.
        '''
        pass


    def __init__(self, host: str ="127.0.0.1", port: int =6379) -> None:
        '''
        Create new Redis Publisher.
        
        args:
        `host`: Redis host IP.
        `port`: Redis port number.
        '''

        self.host = host
        self.port = port

        self.conn = RESP.ClientConnection(self.host, self.port)

        self.lock = asyncio.Lock()

        pass


    async def connect(self) -> None:
        '''
        Connect to Redis server.

        exceptions:
        `ConnectionRefused`: connection to the Redis server refused.

        return: None.
        '''
        try:
            await self.conn.connect()
        except:
            raise Publisher.ConnectionRefused
        return


    async def publish(self, channel: str, message: str, timeout: int =None) -> int:
        '''
        Publish `message` on `channel`.

        args:
        `channel`: channel name of type `str`.
        `message`: message of type `str`.
        `timeout`: timeout period in seconds. if None, no timeout is imposed.

        exceptions:
        `ValueError`: args should be of type `str`.
        `ParsingError`: got an unexpected response from the server.
        `TimeoutError`: Waiting for response timed out.
        `ClientDisconnected`: Client disconnected from server.
        `ServerError`: server sent an error response.
        `Error`: server sent an unexpected RESP response type.

        return:
        `int`: number of subscribers on `channel`.
        '''

        if not isinstance(channel, str):
            raise Publisher.ValueError("Channel should be of type str, got " + type(channel).__name__)

        if not isinstance(message, str):
            raise Publisher.ValueError("Message should be of type str, got " + type(message).__name__)

        array = RESP.Array([RESP.BulkString("PUBLISH"), RESP.BulkString(channel), RESP.BulkString(message)])

        async with self.lock:

            try:
                await self.conn.send(array)
                ret = await self.conn.receive(timeout=timeout)

            except RESP.ClientConnection.ParsingError:
                await self.conn.disconnect()
                raise Publisher.ParsingError

            except RESP.ClientConnection.ClientDisconnected:
                await self.conn.disconnect()
                raise Publisher.ClientDisconnected

            except RESP.ClientConnection.TimeoutError:
                await self.conn.disconnect()
                raise Publisher.TimeoutError


            if not isinstance(ret, RESP.Integer):
                if not isinstance(ret, RESP.Error):
                    await self.conn.disconnect()
                    raise Publisher.ServerError(str(ret))
                
                await self.conn.disconnect()
                raise Publisher.Error("Server sent an unexpected RESP type: " + type(ret).__name__)

            return int(ret)
            




class Subscriber():
    pass



if __name__ == "__main__":

    async def main():

        pub = Publisher("127.0.0.1", 6379)

        await pub.connect()

        i = 0

        while True:
            print(str(i) + " -> " + str(await pub.publish("test", "{type: \"message\"}")))
            i += 1

 
    asyncio.run()