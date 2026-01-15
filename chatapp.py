from textual import on
from textual.app import App, ComposeResult
from textual.widgets import TextArea, Input, ListView, ListItem, Label, Footer
from textual.containers import Vertical, Horizontal

from urllib.parse import quote
from time import sleep
import asyncio
import sys
import msgpack


class ChatLikeApp(App):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.user_list: ListView
        self.history: TextArea
        self.nick_name: str = "David"
        self.channel_number: int = 1

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

    async def render_user_list(
        self, chan_id: int, add_users: dict[int, str], remove_users: dict[int, str]
    ):
        for a in remove_users.keys():
            try:
                item = self.user_list.query_one(f"#user-{a}", ListItem)
                item.remove()
            except Exception:
                pass

        for id, name in add_users.items():
            self.user_list.append(ListItem(Label(f"{name}"), id=f"user-{id}"))

        self.user_list.sort_children(
            key=lambda item: str(item.query_one(Label).content).casefold()
        )

    def compose(self) -> ComposeResult:
        yield Horizontal(
            ListView(id="user-list"),
            Footer(),
            Vertical(
                TextArea(id="history", read_only=True, name="Booba"),
                Input(id="entry", placeholder="Type here and press Enter..."),
            ),
        )

    @on(ListView.Selected)
    def user_selected(self, event: ListView.Selected):
        self.history.insert(str(event.__dict__))

    def on_mount(self) -> None:
        self.query_one("#entry", Input).focus()
        self.history = self.query_one("#history", TextArea)

    def on_input_submitted(self, event: Input.Submitted) -> None:
        text = event.value.strip()
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
