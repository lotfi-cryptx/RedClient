import asyncio
from dataclasses import dataclass

from . import RESP

class Publisher():

    class Error(Exception):
        '''
        Base class for Publisher exceptions.
        '''
        pass

    class ConnectionRefusedError(Error):
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

    class ConnectionClosedError(Error):
        '''
        Raised when the client is not connected to the Redis server.
        '''
        pass

    class ParsingError(Error):
        '''
        Raised when an error occurs when parsing data received from the server.
        '''
        pass

    class UnexpectedResponseError(Error):
        '''
        Raised when an unexpected response is received from the server.
        '''
        pass


    def __init__(self) -> None:
        '''
        Create new Redis Publisher object.
        '''

        self._host = None
        self._port = None

        self._conn = RESP.Connection()

        self._lock = asyncio.Lock()

        pass


    async def connect(self, host: str ="127.0.0.1", port: int =6379) -> None:
        '''
        Connect to Redis server.

        args:
        `host`: Redis server host.
        `port`: Redis server port.

        exceptions:
        `ConnectionRefusedError`: connection to the Redis server refused.
        '''

        try:
            await self._conn.connect(host, port)

        except RESP.Connection.ConnectionRefusedError:
            raise self.ConnectionRefusedError

        return


    async def publish(self, channel: str, message: str, timeout: int =None) -> int:
        '''
        Publish `message` on `channel`.

        args:
        `channel`: channel name.
        `message`: message to publish.
        `timeout`: timeout period in seconds. if None, no timeout is imposed.

        exceptions:
        `ValueError`: Args should be of type `str`.
        `TimeoutError`: Waiting for response timed out.
        `ConnectionClosedError`: Connection to server closed.
        `ParsingError`: Error while parsing received data from server.
        `UnexpectedResponseError`: Received unexpected response from server (connection remains open).

        return:
        Number of subscribers on `channel`.
        '''

        if not isinstance(channel, str):
            raise self.ValueError("expected channel type str, got " + type(channel).__name__)

        if not isinstance(message, str):
            raise self.ValueError("expected message type str, got " + type(message).__name__)

        if not self._conn.connected():
            await self._conn.disconnect()
            raise self.ConnectionClosedError

        array = RESP.Array([RESP.BulkString("PUBLISH"), RESP.BulkString(channel), RESP.BulkString(message)])

        async with self._lock:

            try:
                await self._conn.send(array)
                ret = await self._conn.receive(timeout=timeout)

            except RESP.Connection.ParsingError:
                await self._conn.disconnect()
                raise self.ParsingError

            except RESP.Connection.ConnectionClosedError:
                await self._conn.disconnect()
                raise self.ConnectionClosedError

            except RESP.Connection.TimeoutError:
                await self._conn.disconnect()
                raise self.TimeoutError

            if not isinstance(ret, RESP.Integer):
                raise self.UnexpectedResponseError(f"Server sent unexpected response: {str(ret)}")

            return int(ret)




class Subscriber(object):

    class Error(Exception):
        '''
        Base class for Subscriber exceptions.
        '''
        pass

    class ConnectionRefusedError(Error):
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

    class ConnectionClosedError(Error):
        '''
        Raised when the client is not connected to the Redis server.
        '''
        pass

    class ParsingError(Error):
        '''
        Raised when an error occurs when parsing data received from the server.
        '''
        pass

    class UnexpectedResponseError(Error):
        '''
        Raised when an unexpected response is received from the server.
        '''
        pass


    @dataclass
    class QSubscribed():
        channel: str

    @dataclass
    class QMessage():
        channel: str
        data: bytes

    @dataclass
    class QUnsubscribed():
        channel: str


    def __init__(self) -> None:

        self._conn = RESP.Connection()

        self._lock = asyncio.Lock()

        self._pending: dict[str, list[asyncio.Queue]] = {}
        self._subscribed: dict[str, list[asyncio.Queue]] = {}

        pass



    async def connect(self, host: str ="127.0.0.1", port: int =6379) -> None:
        '''
        Connect to Redis server.

        args:
        `host`: Redis server host.
        `port`: Redis server port.

        exceptions:
        `ConnectionRefusedError`: connection to the Redis server refused.
        '''
        try:
            await self._conn.connect(host, port)

        except RESP.Connection.ConnectionRefusedError:
            await self._conn.disconnect()
            raise self.ConnectionRefusedError

        return



    async def subscribe(self, channel: str, q: asyncio.Queue) -> None:
        '''
        Subscribe to `channel`, and bind it with `q`.
        When subscribtion is confirmed from server, a QSubscribed is pushed to `q`.
        Messages will be pushed as QMessage objects.

        args:
        `channel`: channel to subscribe to.
        `q`: queue where subscribtion and data messages will be pushed for the specified channel.

        exceptions:
        `ValueError`: Invalid argument types.
        `ConnectionClosedError`: Connection to server closed
        '''

        if not isinstance(channel, str):
            raise self.ValueError(f"channel should be a string. got {type(channel).__name__}")

        if not isinstance(q, asyncio.Queue):
            raise self.ValueError(f"q should be an asyncio.Queue object. got {type(q).__name__}")

        if channel in self._pending.keys():
            self._pending[channel].append(q)
            return

        if channel in self._subscribed.keys():
            self._subscribed[channel].append(q)
            await q.put(self.QSubscribed(channel=channel))
            return

        arr = RESP.Array([RESP.BulkString("SUBSCRIBE"), RESP.BulkString(channel)])

        async with self._lock:
            try:
                await self._conn.send(arr)

            except RESP.Connection.ConnectionClosedError:
                #await self._unsubscribe_all()
                await self._conn.disconnect()
                raise self.ConnectionClosedError
            
            self._pending[channel] = [q]



    async def run_forever(self) -> None:
        '''
        Handle subscribtion and data messages sent from the server.

        exceptions:
        `ConnectionClosedError`: Connection with the server closed.
        `ParsingError`: Error occured while parsing data received from server.
        `UnexpectedResponseError`: Server sent unexpected response.
        '''

        while True:

            try:
                ret = await self._conn.receive()

            except RESP.Connection.ConnectionClosedError:
                await self._conn.disconnect()
                raise self.ConnectionClosedError

            except RESP.Connection.ParsingError:
                await self._conn.disconnect()
                raise self.ParsingError


            if not isinstance(ret, RESP.Array):
                await self._conn.disconnect()
                raise self.UnexpectedResponseError

            if len(ret) != 3:
                await self._conn.disconnect()
                raise self.UnexpectedResponseError

            if not isinstance(ret[0], RESP.BulkString):
                await self._conn.disconnect()
                raise self.UnexpectedResponseError


            if ret[0].value() == b'subscribe':

                if not isinstance(ret[1], RESP.BulkString) or not isinstance(ret[2], RESP.Integer):
                    await self._conn.disconnect()
                    raise self.UnexpectedResponseError

                try:
                    channel = ret[1].value().decode('ascii')
                except UnicodeDecodeError:
                    await self._conn.disconnect()
                    raise self.UnexpectedResponseError

                if not channel in self._pending.keys():
                    await self._conn.disconnect()
                    raise self.UnexpectedResponseError

                ch_q = self._pending.pop(channel)

                self._subscribed[channel] = ch_q

                for q in ch_q:
                    await q.put(self.QSubscribed(channel=channel))


            elif ret[0].value() == b'message':
                
                if not isinstance(ret[1], RESP.BulkString) or not isinstance(ret[2], RESP.BulkString):
                    await self._conn.disconnect()
                    raise self.UnexpectedResponseError

                try:
                    channel = ret[1].value().decode('ascii')
                except UnicodeDecodeError:
                    await self._conn.disconnect()
                    raise self.UnexpectedResponseError

                if not channel in self._subscribed.keys():
                    await self._conn.disconnect()
                    raise self.UnexpectedResponseError

                for q in self._subscribed[channel]:
                    await q.put(self.QMessage(channel=channel, data=ret[2].value()))


            elif ret[0].value() == b'unsubscribe':
                
                if not isinstance(ret[1], RESP.BulkString) or not isinstance(ret[2], RESP.Integer):
                    await self._conn.disconnect()
                    raise self.UnexpectedResponseError

                try:
                    channel = ret[1].value().decode('ascii')
                except UnicodeDecodeError:
                    await self._conn.disconnect()
                    raise self.UnexpectedResponseError

                if not channel in self._subscribed.keys():
                    await self._conn.disconnect()
                    raise self.UnexpectedResponseError

                ch_q = self._subscribed.pop(channel)

                for q in ch_q:
                    await q.put(self.QUnsubscribed(channel=channel))

            else:
                await self._conn.disconnect()
                raise self.UnexpectedResponseError



    async def unsubscribe(self, channel: str, q: asyncio.Queue) -> None:
        '''
        Unsubscribe from `channel` which is bound to `q`.
        When unsubscribtion is confirmed from server, a `QUnsubscribed` is pushed to `q`.
        if `channel` has other queues bound to it, the `QUnsubscribe` is immediatly pushed to `q`.

        args:
        `channel`: channel to unsubscribe from.
        `q`: queue to be removed from `channel`'s bindind list.

        exceptions:
        `ValueError`: Invalid argument types.
        `ConnectionClosedError`: Connection to server closed
        '''

        if not isinstance(channel, str):
            raise self.ValueError(f"channel should be a string. got {type(channel).__name__}")

        if not isinstance(q, asyncio.Queue):
            raise self.ValueError(f"q should be an asyncio.Queue object. got {type(q).__name__}")

        if not channel in self._subscribed.keys():
            return

        if not q in self._subscribed[channel]:
            return


        if len(self._subscribed[channel]) > 1:
            self._subscribed[channel].remove(q)
            await q.put(self.QUnsubscribed(channel=channel))
            return


        arr = RESP.Array([RESP.BulkString("UNSUBSCRIBE"), RESP.BulkString(channel)])

        async with self._lock:
            try:
                await self._conn.send(arr)

            except RESP.Connection.ConnectionClosedError:
                await self.conn.disconnect()
                raise self.ConnectionClosedError

        pass
