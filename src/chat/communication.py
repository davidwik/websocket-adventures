from collections import deque
from chat.protocoltypes import Message, PackIDX, Command
from chat.utils import mk_pack
import msgpack
import asyncio
import websockets


class Consumer:
    def __init__(self, ws: websockets.ClientConnection):
        self.listen_task: asyncio.Task
        self.ws = ws
        self.msg_in = 0
        self.queue: asyncio.Queue[Message] = asyncio.Queue()

    async def start(self):
        self.listen_task = asyncio.create_task(self.listener())

    async def listener(self):
        try:
            async for raw in self.ws:
                self.msg_in += 1
                msg = msgpack.unpackb(raw, strict_map_key=False)
                message = Message(
                    Command(int(msg[PackIDX.COMMAND])),
                    int(msg[PackIDX.ID]),
                    msg[PackIDX.NAME],
                    msg[PackIDX.CONTENT],
                    int(msg[PackIDX.CHANNEL]),
                    int(msg[PackIDX.TO_ID]),
                    float(msg[PackIDX.TS]),
                )
                await self.queue.put(message)
        finally:
            await self.ws.close()


class Producer:
    def __init__(self, ws: websockets.ClientConnection):
        self.writer_task: asyncio.Task
        self.ws = ws
        self.queue: asyncio.Queue[Message] = asyncio.Queue()
        self.closed = False
        self.msg_sent = 0

    async def start(self):
        self.writer_task = asyncio.create_task(self.writer())

    async def writer(self):
        try:
            while True:
                message: Message = await self.queue.get()
                msg = mk_pack(**message.dict())
                msg = await self.ws.send(msg)
                self.msg_sent += 1
        except websockets.ConnectionClosed:
            pass
        finally:
            self.closed = True


class Log:
    def __init__(self):
        self.channels: dict[int, deque[Message]] = {}
        self.chats: dict[int, deque[Message]] = {}

    def get_channel(self, id: int) -> deque:
        if id in self.channels:
            return self.channels[id]
        self.channels[id] = deque()
        return self.channels[id]

    def get_chat(self, id: int) -> deque:
        if id in self.chats:
            return self.chats[id]
        self.chats[id] = deque()
        return self.chats[id]
