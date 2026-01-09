import asyncio
import websockets
import sys
from multiuser import Msg


async def main(username: str):
    id = None
    async with websockets.connect(
        "ws://localhost:8765",
        ping_timeout=None,
        close_timeout=None,
        additional_headers={"username": username},
    ) as websocket:
        while id is None:
            id = await websocket.recv()
        id = int(id)
        while True:
            print(f"id: {id}")
            message = input("Enter message type `quit` to exit: ")
            if message.lower() == "quit":
                break
            msg = Msg(id, username, message)

            await websocket.send(msg.pack())
            response = await websocket.recv()
            res = Msg.unpack(response)
            print(res.data)
        await websocket.close()


if __name__ == "__main__":
    if not sys.argv[1]:
        print("python client.py [USERNAME]")
        sys.exit(1)
    name = sys.argv[1]

    asyncio.run(main(name))
