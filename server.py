import asyncio
from enum import IntEnum
import websockets
from snow import Snowflake
from time import time
import sys
import msgpack

from urllib.parse import parse_qs, urlparse

MAX_TOTAL_QUEUE = 100000  # max total queued messages across all clients
MAX_QUEUE_PER_CLIENT = 500  # per-client queue size


class Command(IntEnum):
    REGISTER = 1
    JOIN_CHANNEL = 2
    WRITE_TO_CHANNEL = 3
    WRITE_TO_USER = 4
    GET_CHAN_LIST = 5
    CHAN_LIST_DATA = 6


class PackIDX(IntEnum):
    COMMAND = 0
    ID = 1
    NAME = 2
    CONTENT = 3
    CHANNEL = 4
    TO_ID = 5
    TS = 6


def mk_pack(
    command: Command,
    id: int,
    name: str,
    content: str | dict[int, str] | list[tuple],
    chan: int = 0,
    to: int = 0,
    ts: float | None = None,
) -> bytes:
    if ts is None:
        ts = time()
    return msgpack.packb((command, id, name, content, chan, to, ts))  # type: ignore


class ClientSession:
    def __init__(self, ws, server):
        self.ws = ws

        self.server = server
        self.queue = asyncio.Queue(maxsize=MAX_QUEUE_PER_CLIENT)
        self.writer_task = asyncio.create_task(self.writer())
        self.closed = False
        self.channels: set[int] = set()

    async def writer(self):
        try:
            while True:
                msg = await self.queue.get()
                await self.ws.send(msg)
                self.server.msg_out += 1
        except websockets.ConnectionClosed:
            pass
        finally:
            self.closed = True


class WebsocketServer:
    def __init__(self):
        self.clients = {}
        self.channels: dict[int, dict[int, str]] = {
            0: {},
            1: {},
            2: {},
            3: {},
            4: {},
            5: {},
            6: {},
            7: {},
            8: {},
            9: {},
            10: {},
        }
        self.sf = Snowflake(22)
        self.msg_in: int = 0
        self.msg_out: int = 0
        self.last_msg_in = 0
        self.last_msg_out = 0
        self.total_queued_items = 0

    async def register(self, ws, name):
        total_queued = sum(s.queue.qsize() for s in self.clients.values())

        if total_queued > MAX_TOTAL_QUEUE:
            try:
                await ws.close(code=1013, reason="Server exhausted")
                print("Server exhausted, stopping new clients")
            except websockets.ConnectionClosedError:
                print("Failed to write to client")
                return None
            except websockets.ConnectionClosedOK:
                return None

        id = self.sf.next_id()
        session = ClientSession(ws, self)
        self.clients[id] = session
        await session.queue.put(str(id))
        return id

    async def unregister(self, id):
        session = self.clients.pop(id)
        _ = [self.channels[a].pop(id) for a in session.channels]
        if session:
            session.writer_task.cancel()
            try:
                await session.writer_task
            except asyncio.CancelledError:
                pass

    async def register_on_channel(self, ws, id: int, chan_id: int, name: str):
        self.channels[chan_id][id] = str(name)
        session: ClientSession = self.clients[id]
        session.channels.add(chan_id)
        await session.queue.put(str(chan_id))

    async def send_chan_list(self, ws, msg):
        command = Command.CHAN_LIST_DATA
        id = msg[PackIDX.ID]
        session: ClientSession = self.clients[id]
        chan_id = msg[PackIDX.CHANNEL]
        content = self.channels[chan_id].copy()
        new_msg = mk_pack(command, id, msg[PackIDX.NAME], content, chan_id, to=id)
        print("REQ for channel list")
        await session.queue.put(new_msg)

    async def write_to_channel(self, msg: tuple):
        chan = int(msg[PackIDX.CHANNEL])
        id = int(msg[PackIDX.ID])
        message = mk_pack(*msg)
        for a in list(self.channels[chan].keys()):
            session = self.clients[a]
            try:
                await session.queue.put(message)
            except asyncio.QueueFull:
                await self.unregister(id)

    async def broadcast(self, message: bytes):
        for client_id, session in list(self.clients.items()):
            try:
                await session.queue.put(message)
            except asyncio.QueueFull:
                print(f"Client {client_id} is too slow, disconnecting.")
                await self.unregister(client_id)

    async def stats_printer(self):
        while True:
            await asyncio.sleep(1)
            delta_in = self.msg_in - self.last_msg_in
            delta_out = self.msg_out - self.last_msg_out

            total_clients = len(self.clients)
            total_queued = sum(
                session.queue.qsize() for session in self.clients.values()
            )

            f = ""
            for x, y in self.channels.items():
                f += f"[CHANNEL {x}] = {len(y)}\n"
            print(f)

            self.total_queued_items = total_queued
            print(
                f"-----\nclients={total_clients}"
                f"\ntotal_queued_msgs={total_queued}"
                f"\nin={delta_in}/s"
                f"\nout={delta_out}/s"
            )
            self.last_msg_in = self.msg_in
            self.last_msg_out = self.msg_out

    async def handle_connection(self, ws):
        # Check if the server status is not OVERLOADED
        query = urlparse(ws.request.path).query
        name = parse_qs(query)["username"][0]

        id = await self.register(ws, name)
        if id is None:
            return
        try:
            async for raw in ws:
                self.msg_in += 1
                msg = msgpack.unpackb(raw)
                match msg[PackIDX.COMMAND]:
                    case Command.JOIN_CHANNEL:
                        chan_id = msg[PackIDX.CHANNEL]
                        await self.register_on_channel(ws, id, chan_id, name)
                        continue
                    case Command.WRITE_TO_CHANNEL:
                        chan_id = msg[PackIDX.CHANNEL]
                        await self.write_to_channel(msg)
                        continue
                    case Command.GET_CHAN_LIST:
                        await self.send_chan_list(ws, msg)
                        continue
                    case _:
                        print(f"Well this is awkward{msg}")
                        continue

        finally:
            await self.unregister(id)


async def main():
    server = WebsocketServer()

    port = sys.argv[1]

    async def handle_safe(ws):
        try:
            await server.handle_connection(ws)
        except websockets.ConnectionClosedError:
            pass
        except Exception as e:
            print(f"Unexpected error: {e}")

    async with websockets.serve(server.handle_connection, "localhost", int(port)):
        asyncio.create_task(server.stats_printer())
        print("Server started on ws://localhost:8765")
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
