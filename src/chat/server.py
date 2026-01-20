from urllib.parse import parse_qs, urlparse
import asyncio
import websockets
import sys
import msgpack
import uvloop

from chat.utils import mk_pack
from chat.protocoltypes import Command, PackIDX, Snowflake


MAX_QUEUE_PER_CLIENT = 500  # Reduced - clients must keep up
MAX_INBOUND_PER_CLIENT = 100  # Limit inbound processing queue
MAX_CLIENTS = 10000  # Hard limit on concurrent clients


class ClientSession:
    __slots__ = (
        "ws",
        "server",
        "inbound",
        "queue",
        "closed",
        "channels",
        "writer_task",
        "processor_task",
        "id",
    )

    def __init__(self, ws, server):
        self.ws = ws
        self.server: ChatServer = server
        self.inbound = asyncio.Queue(maxsize=MAX_INBOUND_PER_CLIENT)
        self.queue = asyncio.Queue(maxsize=MAX_QUEUE_PER_CLIENT)
        self.closed = False
        self.channels: set[int] = set()
        self.writer_task = asyncio.create_task(self.writer())
        self.processor_task = asyncio.create_task(self.processor())

    async def processor(self):
        loop = asyncio.get_event_loop()
        try:
            while True:
                raw = await self.inbound.get()
                msg = await loop.run_in_executor(None, msgpack.unpackb, raw)
                msg = msgpack.unpackb(raw, strict_map_key=False)
                # msg = await asyncio.to_thread(msgpack.unpackb, raw)
                await self.server.handle_message(self, msg)

        except Exception as e:
            print(f"Processor error {e}")

    async def writer(self):
        """Write messages with batching"""
        try:
            while True:
                msg = await self.queue.get()

                # Send immediately
                try:
                    await self.ws.send(msg)
                    self.server.msg_out += 1
                except websockets.ConnectionClosed:
                    break
                except Exception as e:
                    print(f"Send error: {e}")
                    break

                # Opportunistically send more from queue without waiting
                sent_count = 1
                while sent_count < 20 and not self.queue.empty():
                    try:
                        next_msg = self.queue.get_nowait()
                        await self.ws.send(next_msg)
                        self.server.msg_out += 1
                        sent_count += 1
                    except asyncio.QueueEmpty:
                        break
                    except websockets.ConnectionClosed:
                        return
                    except Exception:
                        break

                # Yield after batch
                if sent_count > 5:
                    await asyncio.sleep(0)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            print(f"Writer error: {e}")
        finally:
            self.closed = True

            # async def writer(self):

    #     try:
    #         while True:
    #             msg = await self.queue.get()

    #             try:
    #                 await asyncio.wait_for(self.ws.send(msg), timeout=5.0)
    #                 self.server.msg_out += 1

    #             except asyncio.TimeoutError:
    #                 print("Send timeout client might be slow..")
    #                 break

    #     except asyncio.CancelledError:
    #         pass
    #     except Exception as e:
    #         print(f"Writer error: {e}")
    #     finally:
    #         self.closed = True


class ChatServer:
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
        # total_queued = sum(s.queue.qsize() for s in self.clients.values())
        """
        if total_queued > MAX_TOTAL_QUEUE:
            try:
                await ws.close(code=1013, reason="Server exhausted")
                print("Server exhausted, stopping new clients")
            except websockets.ConnectionClosedError:
                print("Failed to write to client")
                return None
            except websockets.ConnectionClosedOK:
                return None
        """
        id = self.sf.next_id()
        session = ClientSession(ws, self)
        self.clients[id] = session
        try:
            await session.queue.put(str(id))
        except asyncio.QueueFull:
            await self.unregister(id)
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

    async def leave_channel(self, id, chan_id, name: str):
        session: ClientSession = self.clients[id]

        if chan_id not in session.channels:
            return None
        # Send leave message..
        msg = (Command.LEAVE_CHANNEL_RESP, id, name, "LEFT", chan_id, id, None)
        session.channels.discard(chan_id)
        del self.channels[chan_id][id]
        await self.write_to_channel(msg)

    async def register_on_channel(self, id: int, chan_id: int, name: str):
        self.channels[chan_id][id] = str(name)
        session: ClientSession = self.clients[id]
        session.channels.add(chan_id)
        msg = mk_pack(Command.JOIN_CHANNEL_RESP, id, name, "", chan_id, id)
        wlc_msg = (Command.WELCOME_TO_CHANNEL, id, name, "", chan_id, id)
        try:
            await session.queue.put(msg)
        except asyncio.QueueFull:
            await self.unregister(id)

        await self.write_to_channel(wlc_msg)

    async def send_chan_list(self, msg):
        command = Command.CHAN_LIST_RESP
        id = msg[PackIDX.ID]
        session: ClientSession = self.clients[id]
        chan_id = msg[PackIDX.CHANNEL]
        content = self.channels[chan_id].copy()
        new_msg = mk_pack(
            command, id, msg[PackIDX.NAME], content, chan_id, to=id
        )

        try:
            await session.queue.put(new_msg)
        except asyncio.QueueFull:
            await self.unregister(id)

    async def write_to_channel(self, msg: tuple):
        chan = int(msg[PackIDX.CHANNEL])
        id = int(msg[PackIDX.ID])
        message = mk_pack(*msg)

        for i, a in enumerate(self.channels[chan].keys()):
            session = self.clients[a]
            if not session:
                continue
            if not session.queue.full():
                await session.queue.put(message)

    async def broadcast(self, message: bytes):
        for client_id, session in list(self.clients.items()):
            try:
                await session.queue.put(message)
            except asyncio.QueueFull:
                print(f"Client {client_id} is too slow, disconnecting.")
                await self.unregister(client_id)

    # async def stats_printer(self):
    #     while True:
    #         await asyncio.sleep(5)
    #         delta_in = self.msg_in - self.last_msg_in
    #         delta_out = self.msg_out - self.last_msg_out

    #         total_clients = len(self.clients)
    #         total_queued = sum(
    #             session.queue.qsize() for session in self.clients.values()
    #         )

    #         f = ""
    #         for x, y in self.channels.items():
    #             f += f"[CHANNEL {x}] = {len(y)}\n"
    #         print(f)

    #         self.total_queued_items = total_queued
    #         print(
    #             f"-----\nclients={total_clients}"
    #             f"\ntotal_queued_msgs={total_queued}"
    #             f"\nin={delta_in}/s"
    #             f"\nout={delta_out}/s"
    #         )
    #         self.last_msg_in = self.msg_in
    #         self.last_msg_out = self.msg_out

    async def stats_printer(self):
        while True:
            await asyncio.sleep(5)
            delta_in = self.msg_in - self.last_msg_in
            delta_out = self.msg_out - self.last_msg_out

            total_clients = len(self.clients)
            total_queued = sum(
                session.queue.qsize() for session in self.clients.values()
            )

            total_inbound = sum(
                session.inbound.qsize() for session in self.clients.values()
            )

            channel_info = "\n".join(
                f"[CHANNEL {x}] = {len(y)}"
                for x, y in self.channels.items()
                if y
            )
            if channel_info:
                print(channel_info)

            print(
                f"-----\nclients={total_clients}"
                f"\nqueued_out={total_queued}"
                f"\nqueued_in={total_inbound}"
                f"\nin={delta_in}/s ({delta_in / 5:.0f} msg/s)"
                f"\nout={delta_out}/s ({delta_out / 5:.0f} msg/s)"
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
            session: ClientSession = self.clients[id]
            msg_count = 0
            async for raw in ws:
                try:
                    start = asyncio.get_event_loop().time()
                    session.inbound.put_nowait(raw)
                    if msg_count % 10 == 0:
                        await asyncio.sleep(0)
                except asyncio.QueueFull:
                    print("Inbound queue full, dropping messagde")
        finally:
            await self.unregister(id)

    async def handle_message(self, session: ClientSession, msg: tuple):
        self.msg_in += 1
        id = msg[PackIDX.ID]
        name = msg[PackIDX.NAME]
        # print(f"Recv: {Command(msg[PackIDX.COMMAND]).name}")
        match msg[PackIDX.COMMAND]:
            case Command.JOIN_CHANNEL:
                chan_id = msg[PackIDX.CHANNEL]
                await self.register_on_channel(id, chan_id, name)
            case Command.LEAVE_CHANNEL:
                chan_id = msg[PackIDX.CHANNEL]
                await self.leave_channel(id, chan_id, name)
            case Command.WELCOME_TO_CHANNEL:
                await self.write_to_channel(msg)
            case Command.WRITE_TO_CHANNEL:
                chan_id = msg[PackIDX.CHANNEL]
                await self.write_to_channel(msg)
            case Command.LEAVE_CHANNEL_RESP:
                chan_id = msg[PackIDX.CHANNEL]
                await self.write_to_channel(msg)
            case Command.CHAN_LIST:
                await self.send_chan_list(msg)

            case _:
                print(
                    f"Unhandles command: {Command(msg[PackIDX.COMMAND]).name}"
                )


def main():
    uvloop.run(run())


async def run():
    server = ChatServer()

    port = sys.argv[1]

    async def handle_safe(ws):
        try:
            await server.handle_connection(ws)
        except websockets.ConnectionClosedError:
            pass
        except Exception as e:
            print(f"Unexpected error: {e}")

    async with websockets.serve(
        server.handle_connection,
        "localhost",
        int(port),
        ping_interval=None,  # Much longer interval for high load
        ping_timeout=None,  # Match the interval
        close_timeout=5,  # Shorter close timeout
        max_size=2**20,  # 1MB max message size
        compression=None,  # Disable compression to reduce CPU load
        max_queue=32,  # Limit websocket internal queue (new in recent versions)
        write_limit=2**16,  # 64KB write buffer limit
    ):
        asyncio.create_task(server.stats_printer())
        print(f"Server started on ws://localhost:{port}")
        await asyncio.Future()


if __name__ == "__main__":
    main()
