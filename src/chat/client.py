import asyncio
from collections.abc import Callable
import websockets
from urllib.parse import quote
from chat.protocoltypes import Command, Message, User
from chat.communication import Producer, Consumer, Log
from time import sleep

NAME = "David"
CHANNEL = 2


def update_userlist(msg: Message):
    print("UPDATING UI LIST")
    print(f"data{msg}")
    sleep(3)


class Client:
    def __init__(self):
        self.active: bool = True
        self.incoming_task: asyncio.Task
        self.input_task: asyncio.Task
        self.channels: dict[int, dict[int, str]] = {}
        self.producer: Producer
        self.consumer: Consumer
        self.client_id: int
        self.nickname: str
        self.active_chan_id: int
        self.active_priv_id: int
        self.log: Log = Log()
        self.unread_channel: dict[int, int] = {}
        self.all_users: dict[int, User]
        self.callbacks: dict[Command, Callable[[Message], None]] = {}

    async def register(self, ws, name: str, channel: int):
        print("Register on connection...")
        id = await ws.recv()
        if not str(id).isnumeric():
            raise TypeError("Expecting an int as client id")
        self.client_id = int(id)

    async def send(self, chat_msg: Message):
        await self.producer.queue.put(chat_msg)

    async def connect(self):
        self.nickname = NAME
        async with websockets.connect(
            f"ws://localhost:8765?username={quote(NAME)}"
        ) as ws:
            try:
                self.producer = Producer(ws)
                await self.producer.start()
                await self.register(ws, NAME, CHANNEL)
                self.consumer = Consumer(ws)
                await self.consumer.start()
                self.incoming_task = asyncio.create_task(self.handle_incoming())
                self.input_task = asyncio.create_task(self.input_loop())
                self.routine_task = asyncio.create_task(self.routines())

            except websockets.ConnectionClosedError:
                print("CONN ERR")
                await ws.close()
            except websockets.ConnectionClosed:
                print("DONN")
                await ws.close()

            while self.active:
                await asyncio.sleep(0.1)
                if ws.state is websockets.State.CLOSED:
                    break

            self.incoming_task.cancel()
            self.input_task.cancel()
            await ws.close()

    async def input_loop(self):
        await asyncio.sleep(1)
        msg = Message(
            Command.JOIN_CHANNEL,
            self.client_id,
            self.nickname,
            "",
            CHANNEL,
            0,
        )
        await self.send(msg)

    async def routines(self):
        while True:
            await asyncio.sleep(5)
            msg = Message(
                Command.CHAN_LIST,
                self.client_id,
                self.nickname,
                "",
                self.active_chan_id,
                0,
            )
            await self.send(msg)

    async def handle_incoming(self):
        while True:
            msg: Message = await self.consumer.queue.get()
            match msg.command:
                case Command.WRITE_TO_CHANNEL:
                    self.log.get_channel(msg.chan).append(msg)
                    self.print_active_chan(msg)
                case Command.LEAVE_CHANNEL_RESP:
                    self.log.get_channel(msg.chan).append(msg)
                    self.print_active_chan(msg)
                case Command.WELCOME_TO_CHANNEL:
                    self.log.get_channel(msg.chan).append(msg)
                    self.print_active_chan(msg)
                case Command.JOIN_CHANNEL_RESP:
                    self.active_chan_id = msg.chan
                case Command.CHAN_LIST_RESP:
                    if type(msg.content) is dict:
                        self.channels[msg.chan] = msg.content  # type: ignore
                case Command.WRITE_TO_USER:
                    pass
                case _:
                    print("This is unexpected...")
            await self.perform_callback(msg)
            self.consumer.queue.task_done()

    async def perform_callback(self, msg: Message):
        if msg.command in self.callbacks:
            func = self.callbacks[msg.command]
            await asyncio.to_thread(func, msg)

    def print_active_chan(self, msg: Message):
        if self.active_chan_id != msg.chan:
            return None

        match msg.command:
            case Command.LEAVE_CHANNEL_RESP:
                print(f"<< {msg.name} LEFT THE CHANNEL.")
            case Command.WELCOME_TO_CHANNEL:
                print(f">> {msg.name} JOINED US!")
            case Command.WRITE_TO_CHANNEL:
                print(f"{msg.name}: {msg.content}")
            case _:
                pass

    def register_callback(
        self, cmd: Command, callback: Callable[[Message], None]
    ):
        self.callbacks[cmd] = callback


def main():
    client = Client()
    try:
        asyncio.run(client.connect())
    except KeyboardInterrupt:
        print("Quitting...")


if __name__ == "__main__":
    main()
