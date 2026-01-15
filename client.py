import asyncio
import websockets
import msgpack
from collections import deque
from dataclasses import dataclass, asdict
from urllib.parse import quote
from server import mk_pack, Command, PackIDX

NAME = "David"
CHANNEL = 2


@dataclass(slots=True)
class Message:
    command: Command
    id: int
    name: str
    content: str | dict[int | str, str] | list[tuple]
    chan: int = 0
    to: int = 0
    ts: float | None = None


class Consumer:
    def __init__(self, ws: websockets.ClientConnection):
        self.ws = ws
        self.msg_in = 0
        self.queue: asyncio.Queue = asyncio.Queue()
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
        self.ws = ws
        self.queue: asyncio.Queue = asyncio.Queue()
        self.writer_task = asyncio.create_task(self.writer())
        self.closed = False
        self.msg_sent = 0

    async def writer(self):
        try:
            while True:
                msg = await self.queue.get()
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


class Chat:
    def __init__(self):
        self.producer: Producer
        self.consumer: Consumer
        self.client_id: int
        self.nickname: str
        self.active_chan_id: int
        self.active_priv_id: int
        self.log: Log = Log()

    async def register(self, ws, name: str, channel: int):
        id = await ws.recv()
        if not str(id).isnumeric():
            raise TypeError("Expecting an int as client id")
        self.client_id = int(id)

        msg = mk_pack(
            Command.JOIN_CHANNEL,
            self.client_id,
            self.nickname,
            "",
            CHANNEL,
        )

        self.producer = Producer(ws)
        msg = Message(
            Command.JOIN_CHANNEL, self.client_id, self.nickname, "", CHANNEL, 0
        )
        await self.send(msg)
        chan_id = await ws.recv()
        self.active_chan_id = int(chan_id)

    async def send(self, chat_msg: Message):
        msg = mk_pack(**asdict(chat_msg))
        await self.producer.queue.put(msg)

    async def connect(self):
        self.nickname = NAME
        async with websockets.connect(
            f"ws://localhost:8765?username={quote(NAME)}"
        ) as ws:
            try:
                await self.register(ws, NAME, CHANNEL)
                print(f"id: {self.client_id},  channel: {self.active_chan_id}")
                self.consumer = Consumer(ws)
                asyncio.create_task(self.handle_incoming())

            except websockets.ConnectionClosedError:
                print("CONN ERR")
                await ws.close()
            except websockets.ConnectionClosed:
                print("DONN")
                await ws.close()

            await self.input_loop()

    async def input_loop(self):
        while True:
            cmd = await asyncio.to_thread(input, "\nCommand: ")

    async def handle_incoming(self):
        while True:
            msg: Message = await self.consumer.queue.get()

            match msg.command:
                case Command.WRITE_TO_CHANNEL:
                    self.log.get_channel(msg.chan).append(msg)
                    self.print_active_chan(msg)
                case Command.WRITE_TO_USER:
                    pass

            self.consumer.queue.task_done()

    def print_active_chan(self, msg: Message):
        if msg.chan == self.active_chan_id:
            print(f"{msg.name}: {msg.content}")


def main():
    chat = Chat()
    try:
        asyncio.run(chat.connect())
    except KeyboardInterrupt:
        print("Quitting...")


if __name__ == "__main__":
    main()
