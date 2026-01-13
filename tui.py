from turtle import title
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
        self.user_list: ListView
        self.nick_name: str = "David"
        self.snowflake_id: int
        self.channel_number: int = 1
        self.history: TextArea
        self.recv_task: asyncio.Task
        self.active = True
        self.chan_list = {}

    CSS = """
    Screen {
        layout: vertical;
    }

    ListView {
        border: solid red;
        width: 30;
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
            try:
                msg = msgpack.unpackb(message, strict_map_key=False)
                if msg[PackIDX.COMMAND] == Command.CHAN_LIST_DATA:
                    _ = asyncio.create_task(
                        self.update_user_list(
                            msg[PackIDX.CHANNEL], msg[PackIDX.CONTENT]
                        )
                    )
                else:
                    msgstr = f"{msg[PackIDX.NAME]}: {msg[PackIDX.CONTENT]}\n"
                    self.history.insert(f"{msgstr}")

            except Exception as e:
                self.history.insert(f"Something unexpected {e}")

    async def update_user_list(self, chan_id: int, new_list: dict[int, str]):
        if chan_id in self.chan_list:
            old_list = self.chan_list[chan_id]
        else:
            old_list = {}

        diff = old_list.keys() ^ new_list.keys()

        left: set[int] = old_list.keys() & diff
        new: set[int] = diff - left

        merge = old_list | new_list
        add_users = {a: b for a, b in merge.items() if a in new}
        remove_users = {a: b for a, b in merge.items() if a in left}
        self.chan_list[chan_id] = new_list
        await self.render_user_list(chan_id, add_users, remove_users)

    async def render_user_list(
        self, chan_id: int, add_users: dict[int, str], remove_users: dict[int, str]
    ):
        for a in remove_users.keys():
            try:
                item = self.user_list.query_one(f"#user-{a}", ListItem)
                item.remove()
            except Exception:
                pass

        def get_key(item: ListItem):
            self.history.insert(str(item.query_one(Label).content))
            return str(item.query_one(Label).content)

        for id, name in add_users.items():
            self.user_list.append(ListItem(Label(f"{name}"), id=f"user-{id}"))

        self.user_list.sort_children(
            key=lambda item: str(item.query_one(Label).content).casefold()
        )

    async def update_stuff(self, websocket):
        while self.active:
            msg = mk_pack(
                Command.GET_CHAN_LIST,
                self.snowflake_id,
                self.nick_name,
                "",
                self.channel_number,
                self.snowflake_id,
            )
            await websocket.send(msg)
            await asyncio.sleep(10)

    async def start_safe(self):
        self.active = True
        try:
            await self.start_chat()
        except OSError:
            self.history.insert("Failed to connect to server\n")
        except Exception:
            self.history.insert("Unknown exception\n")

    async def start_chat(self):
        async with websockets.connect(
            f"ws://localhost:8765?username={quote(self.nick_name)}",
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

            self.recv_task = asyncio.create_task(self.receive_data(websocket))
            self.update_stuff_task = asyncio.create_task(self.update_stuff(websocket))

            self.notify("You are now connected!")
            self.user_list.clear()
            self.chan_list = {}
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
        self.user_list = self.query_one("#user-list", ListView)

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

    def _find_insert_index(self, list_view: ListView, name: str) -> int:
        name_key = name.casefold()

        for index, item in enumerate(list_view.children):
            label = item.query_one(Label)
            existing_name = str(label).casefold()

            if existing_name > name_key:
                return index
        return len(list_view.children)


if __name__ == "__main__":
    ChatLikeApp().run()
