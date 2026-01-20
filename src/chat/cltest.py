import asyncio
import websockets
from chat.protocoltypes import PackIDX, Command
from chat.utils import mk_pack
from urllib.parse import quote
import sys
import msgpack
import random
import uvloop

CLIENTS = 2000
strings = [
    "Det är jo väldigt fint väder vi har här i Köping",
    "Det var minsann bättre förr",
    "Kalle visste minsann att man inte får köra på skogsstigar med A-traktor",
    "Lisa är vilsen, hon hittar inte hem!",
    "Enligt aftonbladet är det väldigt dårligt att titta på tv..",
    "På min tid fick man minsann vara tacksam för det lilla, om man ens fick det!",
    "En satans massa gamla låtar som bara går om och om igen, det är inte så kul",
    "Min bil har slutat att fungera",
    "Se där! En sax!",
    "Klaras bästa kompis Sara är läbbig",
    "Jag har ett monster i min garderob",
    "I love Pusheen!!",
    "World bank from China!",
    "Den som ens hade monster...",
    "Ja jisses vilket liv...",
]


names = [
    "Johnny",
    "Dennis",
    "NinjaJohan",
    "SarasHund",
    "Digerdøden hansen",
    "Julia",
    "Anna",
    "JohanPistol",
    "GöranHäst",
    "Nina",
    "David",
    "Sven",
    "Torben",
    "Oskar Holiday",
    "Alice Viktorsson",
]

random.randint(0, 99)


def get_name() -> str:
    num = random.randint(10, 199)

    return f"{random.choice(names)}_{num}"


async def simulate_chat():
    id = None
    chan_id = None
    name = f"{random.choice(names)}_{random.randint(0, 100)}"
    port = sys.argv[1]

    async with websockets.connect(
        f"ws://localhost:{port}?username={quote(name)}", ping_interval=None
    ) as websocket:
        ## First get the id
        try:
            id = await websocket.recv()
            if not str(id).isnumeric():
                return None
        except websockets.ConnectionClosedError:
            print("Failed to connect to server.. maybe busy?")
            return None
        id = int(id)
        print(f"Got ID: {id}")
        ## Join a channel
        chan = random.randint(0, 10)
        chan_id = None
        msg = mk_pack(Command.JOIN_CHANNEL, id, name, "", chan)
        await websocket.send(msg)

        try:
            msg = await websocket.recv()
            msg = msgpack.unpackb(msg, strict_map_key=False)
            if msg[PackIDX.COMMAND] == Command.JOIN_CHANNEL_RESP:
                chan_id = msg[PackIDX.CHANNEL]
        except websockets.ConnectionClosedError:
            print("Failed to register on channel..")
            return None

        if chan_id is None:
            print("Failed to get chanid")

        keep_chatting = True

        while keep_chatting:
            delay = random.randint(500, 3000)
            await asyncio.sleep(delay / 100)
            txt = random.choice(strings)
            msg = mk_pack(Command.WRITE_TO_CHANNEL, id, name, txt, int(chan_id))
            await websocket.send(msg)
            # if random.randint(0, 10) == 5:
            #    msg = mk_pack(
            #        Command.LEAVE_CHANNEL, id, name, "leaving", int(chan_id)
            #    )
            #    chan_id = None
            # await websocket.send(msg)

        await websocket.close()


async def run():
    try:
        async with asyncio.TaskGroup() as tg:
            tasks: list[asyncio.Task] = []
            for _ in range(0, CLIENTS):
                await asyncio.sleep(0.02)
                tasks.append(tg.create_task(simulate_chat()))

    except KeyboardInterrupt:
        print("Shutting down...")


def main():
    uvloop.run(run())
    # asyncio.run(run())


if __name__ == "__main__":
    main()
