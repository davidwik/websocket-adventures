import asyncio

from websockets import ConnectionClosed, serve
from websockets.asyncio.server import ServerConnection
from snow import Snowflake
from time import time

import msgpack


def mk_pack(id: int, name: str, text: str, to: int = 0):
    return msgpack.packb((id, name, text, to))


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
        del self.clients[id]
        print(f"Client disconnected. Total clients {len(self.clients)}")

    async def send_message_to_all(self, message: bytes):
        for client in self.clients.copy().items():
            try:
                await client[1].send(message)
            except Exception as e:
                await self.unregister(client[0])
                print(f"Failed to send message to client {e}")

    async def handle_connection(self, websocket: ServerConnection):
        id = self.sf.next_id()
        name: str = websocket.request.headers["username"]  # type: ignore
        await self.register(websocket, id, name)
        try:
            async for message in websocket:
                msg = msgpack.unpackb(message)
                print(f"Received message: {msg.data}")
                await self.send_message_to_all(mk_pack(msg[0], msg[1], msg[2], msg[3]))

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
