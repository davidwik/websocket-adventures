import asyncio
import websockets


async def handle_connection(websocket):
    print("New client connected")
    try:
        async for message in websocket:
            print(f"Received message: {message}")
            await websocket.send(f"Echo: {message}")
    except websockets.ConnectionClosed:
        print("Client disconnected")
    finally:
        await websocket.close()


async def main():
    async with websockets.serve(handle_connection, "localhost", 8765):
        print("Server started on ws://localhost:8765")
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
