import asyncio
import websockets
from multiuser import mk_pack
import random


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
    "Den som ens hade monster...",
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
]

random.randint(0, 99)


def get_name() -> str:
    num = random.randint(10, 199)

    return f"{random.choice(names)}_{num}"


async def main(username: str = "donnie"):
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
            await asyncio.sleep(random.randint(0, 10))

            message = random.choice(strings)

            if message.lower() == "quit":
                break
            msg = Msg(id, username, message)

            await websocket.send(msg.pack())
            response = await websocket.recv()
            res = Msg.unpack(response)
            print(res.data)
        await websocket.close()


if __name__ == "__main__":
    breakpoint()
    print("python client.py [USERNAME]")
    asyncio.run(main())
