from textual.app import App, ComposeResult
from textual.widgets import TextArea, Input, ListView, ListItem, Label, Footer
from textual.containers import Vertical, Horizontal
from hserver import PackIDX, mk_pack, Command
from urllib.parse import quote
from time import sleep
import asyncio
import websockets
import sys
import msgpack


class ChatLikeApp(App):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.nick_name: str = "David"
        self.snowflake_id: int
        self.channel_number: int = 1
        self.history: TextArea
        self.recv_task: asyncio.Task
        self.active = True

    CSS = """
    Screen {
        layout: vertical;
    }

    ListView {
        border: solid red;
        width: 20;
    }

    TextArea {
        height: 1fr;
        border: solid green;
    }

    Input {
        height: 3;
        border: solid blue;
    }
    """

    async def receive_data(self, websocket):
        async for message in websocket:
            msg = msgpack.unpackb(message)
            if msg[PackIDX.COMMAND] == Command.CHAN_LIST_DATA:
                await self.update_user_list(self, msg[PackIDX.CONTENT])
            else:
                msgstr = f"{msg[PackIDX.NAME]}: {msg[PackIDX.CONTENT]}\n"
                self.history.insert(msgstr)

    async def update_user_list(self, user_list: dict[int, str]):
        self.history.insert(str(user_list))

    async def update_stuff(self, websocket):
        while self.active:
            msg = mk_pack(
                Command.GET_CHAN_LIST,
                self.snowflake_id,
                self.nick_name,
                "",
                self.channel_number,
                0,
            )
            await websocket.send(msg)
            await asyncio.sleep(10)

    async def start_safe(self):
        try:
            await self.start_chat()
        except OSError:
            self.history.insert("Failed to connect to server\n")
        except Exception:
            self.history.insert("Unknown exception\n")

    async def start_chat(self):
        self.active = True

        async with websockets.connect(
            f"ws://localhost:8765?username={quote(self.nick_name)}",
            ping_timeout=None,
            close_timeout=None,
        ) as websocket:
            try:
                id = await websocket.recv()
                if not str(id).isnumeric():
                    print(id)
                    return None
            except websockets.ConnectionClosedError:
                self.history.insert("Failed to connect to server.. maybe busy?\n")
                return None
            except websockets.ConnectionClosed:
                self.history.insert("What connec??")

            self.snowflake_id = int(id)

            chan = self.channel_number
            chan_id = None
            msg = mk_pack(
                Command.JOIN_CHANNEL,
                self.snowflake_id,
                self.nick_name,
                "",
                self.channel_number,
            )
            await websocket.send(msg)
            try:
                chan_id = await websocket.recv()
            except websockets.ConnectionClosedError:
                self.history.insert("Failed to register on channel..\n")
                return None
            if chan_id is None:
                self.history.insert("Failed to get chanid\n")

            self.update_stuff_task = asyncio.create_task(self.update_stuff(websocket))
            self.recv_task = asyncio.create_task(self.receive_data(websocket))

            self.notify("You are now connected!")
            self.history.insert(
                f"Connected as [{self.nick_name}] on channel: #{self.channel_number}\n"
            )
            while self.active:
                await asyncio.sleep(0.2)

            self.recv_task.cancel()

            await websocket.close()

    def compose(self) -> ComposeResult:
        yield Horizontal(
            ListView(id="user-list"),
            Footer(),
            Vertical(
                TextArea(id="history", read_only=True, name="Booba"),
                Input(id="entry", placeholder="Type here and press Enter..."),
            ),
        )

    def on_mount(self) -> None:
        self.query_one("#entry", Input).focus()
        self.history = self.query_one("#history", TextArea)
        self.userList: ListView = self.query_one("#user-list", ListView)

        for a in range(0, 100):
            self.userList.append(ListItem(Label("asdf")))

    def on_input_submitted(self, event: Input.Submitted) -> None:
        text = event.value.strip()
        if "/name" in text:
            try:
                self.nick_name = text.split(" ")[1]
                self.history.insert(f"Name set to {self.nick_name}\n")
            except Exception as e:
                self.history.insert("Error setting name\n")
        elif "/channel" in text:
            try:
                self.channel_number = int(text.split(" ")[1])
                self.history.insert(f"Channel set to {self.channel_number}\n")
            except Exception:
                self.history.insert("Error...\n")
        elif "/connect" == text:
            if self.nick_name == "" or not self.channel_number:
                self.history.insert(
                    "You need to set a nickname and a channelnumber "
                    "first\n/name [NAME]\n/channel [CHAN_NUM]\n"
                )
            else:
                self.history.insert("Connecting...\n")
                loop = asyncio.get_event_loop()
                loop.create_task(self.start_safe())
        elif "/quit" == text:
            self.active = False
            self.history.clear()
        elif "/clear" == text:
            self.history.clear()
        elif "/exit" == text:
            self.active = False
            sleep(2)
            sys.exit()
        else:
            if text:
                self.history.insert(f"{text}\n")

        event.input.value = ""  # Clear input


if __name__ == "__main__":
    ChatLikeApp().run()
