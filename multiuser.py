import asyncio
from typing import BinaryIO

from websockets import ConnectionClosed, serve
from websockets.asyncio.server import ServerConnection
from snow import Snowflake
from time import time

import msgpack


def mk_pack(id: int, name: str, text: str, to: int = 0, ts=None) -> bytes:
    if ts is None:
        ts = time()
    return msgpack.packb((id, name, text, ts, to))  # type: ignore


class WebsocketServer:
    def __init__(self):
        self.clients: dict[int, ServerConnection] = {}
        self.users: dict[int, str] = {}
        self.sf: Snowflake = Snowflake(22)

    async def register(self, websocket: ServerConnection, id: int, name: str):
        self.clients[id] = websocket
        self.users[id] = name
        await self.clients[id].send(str(id))
        # await self.send_message_to_all(f"{name} joined!")
        print(f"New client connected. Total clients {len(self.clients)}")

    async def unregister(self, id: int):
        if id in self.clients:
            del self.clients[id]
            del self.users[id]
            print(f"Client disconnected. Total clients {len(self.clients)}")

    async def send_message_to_all(self, id, websocket, message: bytes):
        tasks = []
        for id, ws in list(self.clients.items()):
            tasks.append(self._safe_send(id, websocket, message))
        await asyncio.gather(*tasks)

    async def _safe_send(self, id: int, websocket, message: bytes):
        try:
            await websocket.send(message)
        except ConnectionClosed:
            await self.unregister(id)

    async def handle_connection(self, websocket: ServerConnection):
        id = self.sf.next_id()
        name: str = websocket.request.headers["username"]  # type: ignore
        await self.register(websocket, id, name)
        try:
            async for message in websocket:
                msg = msgpack.unpackb(message)
                print(msg)
                msg = mk_pack(msg[0], msg[1], msg[2], msg[3], msg[4])
                await self.send_message_to_all(id, websocket, msg)

        except ConnectionClosed:
            print("Client disconnected")
        finally:
            await self.unregister(id)


async def main():
    server = WebsocketServer()
    async with serve(server.handle_connection, "localhost", 8765):
        print("Server started on ws://localhost:8765")
        await asyncio.Future()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Shutting down...")
