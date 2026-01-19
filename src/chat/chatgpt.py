import asyncio
import websockets
import msgpack
from collections import defaultdict

# Simple command identifiers
JOIN = 1
LEAVE = 2
MESSAGE = 3


class ClientSession:
    def __init__(self, ws, server):
        self.ws = ws
        self.server = server

        # Queue for messages this client needs to send
        self.send_queue = asyncio.Queue(maxsize=100)

        # Queue for raw messages received from this client
        self.inbound_queue = asyncio.Queue(maxsize=100)

        # Start writer and processor tasks
        self.writer_task = asyncio.create_task(self.writer())
        self.processor_task = asyncio.create_task(self.processor())

        # Channels this client is joined to
        self.channels = set()
        self.closed = False

    async def writer(self):
        """Send messages from send_queue to websocket"""
        try:
            while True:
                msg = await self.send_queue.get()
                await self.ws.send(msg)
        except websockets.ConnectionClosed:
            pass
        finally:
            self.closed = True

    async def processor(self):
        """Process inbound messages and call server logic"""
        try:
            while True:
                raw = await self.inbound_queue.get()
                msg = msgpack.unpackb(raw, strict_map_key=False)
                await self.server.handle_message(self, msg)
        except Exception as e:
            print(f"Processor error: {e}")


class ChatServer:
    def __init__(self):
        self.clients = set()
        self.channels = defaultdict(set)  # channel_id -> set of ClientSession

    async def register(self, ws):
        session = ClientSession(ws, self)
        self.clients.add(session)
        return session

    async def unregister(self, session):
        self.clients.discard(session)
        for chan_id in session.channels:
            self.channels[chan_id].discard(session)
        session.writer_task.cancel()
        session.processor_task.cancel()
        try:
            await session.writer_task
        except asyncio.CancelledError:
            pass
        try:
            await session.processor_task
        except asyncio.CancelledError:
            pass

    async def handle_connection(self, ws):
        session = await self.register(ws)
        try:
            async for raw in ws:
                # Cheap, fast enqueue — never block here
                try:
                    session.inbound_queue.put_nowait(raw)
                except asyncio.QueueFull:
                    print("Inbound queue full, dropping message")
        finally:
            await self.unregister(session)

    async def handle_message(self, session: ClientSession, msg: tuple):
        """Handle unpacked messages"""
        command = msg[0]
        if command == JOIN:
            chan_id = msg[1]
            self.channels[chan_id].add(session)
            session.channels.add(chan_id)
            print(f"Client joined channel {chan_id}")
        elif command == LEAVE:
            chan_id = msg[1]
            self.channels[chan_id].discard(session)
            session.channels.discard(chan_id)
            print(f"Client left channel {chan_id}")
        elif command == MESSAGE:
            chan_id, text = msg[1], msg[2]
            # Fanout: non-blocking per client
            packed = msgpack.packb((MESSAGE, chan_id, text))
            for client in list(self.channels[chan_id]):
                try:
                    client.send_queue.put_nowait(packed)
                except asyncio.QueueFull:
                    print("Send queue full, disconnecting slow client")
                    asyncio.create_task(self.unregister(client))


async def main():
    server = ChatServer()
    async with websockets.serve(server.handle_connection, "localhost", 8765):
        print("Chat server running at ws://localhost:8765")
        await asyncio.Future()  # run forever


if __name__ == "__main__":
    asyncio.run(main())
