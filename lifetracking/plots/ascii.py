from __future__ import annotations

import shutil
from datetime import datetime, timedelta

from rich import print


def get_terminal_size() -> tuple[int, int]:
    """Returns the size of the terminal in columns and lines"""

    size = shutil.get_terminal_size((80, 20))
    return size.columns, size.lines - 6


def get_base_text() -> list[list[str]]:
    """Returns a 2D list of strings with the same dimensions as the terminal"""

    w, h = get_terminal_size()
    return [[" " for _ in range(w)] for _ in range(h)]


def draw_checkerboard(
    texts: list[list[str]],
    character: str = "█",
    color: str | None = "#7a3135",
) -> list[list[str]]:
    """Draws a checkerboard pattern on the 2D list of strings"""

    assert isinstance(texts, list), "The texts must be a list of lists of strings"
    assert isinstance(character, str), "The character must be a string"
    assert isinstance(color, str), "The color must be a string"
    assert color.startswith("#"), "The color must be in hex format"
    assert len(color) == 7, "The color must be in hex format"

    for x in range(len(texts[0])):
        for y in range(len(texts)):
            if (x + y) % 2 == 1:
                t = character
                if color is not None and color != "":
                    t = f"[{color}]{character}[/]"
                texts[y][x] = t
    return texts


def draw_columns(
    texts: list[list[str]],
    character: str = "█",
    color: str | None = None,
    column_width: int = 5,
) -> list[list[str]]:
    """Draws background of columns"""

    assert isinstance(texts, list), "The texts must be a list of lists of strings"
    assert isinstance(character, str), "The character must be a string"
    assert isinstance(color, str) or color is None, "The color must be a string"
    # assert color.startswith("#"), "The color must be in hex format"
    # assert len(color) == 7, "The color must be in hex format"
    assert isinstance(column_width, int), "The column width must be an integer"
    assert column_width % 2 == 1, "The column width needs to be odd"

    w, h = len(texts[0]), len(texts)

    total_columns = w // (column_width + 1)
    left_paddings = (w % (column_width + 1)) // 2
    for i in range(total_columns):
        for j in range(h):
            t = character
            if color is not None and color != "":
                t = f"[{color}]{character}[/]"
            texts[j][(i * (column_width + 1)) + left_paddings + column_width // 2] = t

    return texts


def draw_time_segment(
    texts: list[list[str]],
    seg: dict,
    color: str | None = "#51afcf",
    column_width: int = 5,
) -> list[list[str]]:

    if isinstance(color, str):
        color = color.strip()

    w, h = len(texts[0]), len(texts)
    total_columns = w // (column_width + 1)
    left_paddings = (w % (column_width + 1)) // 2

    s = seg["start"]
    assert isinstance(s, datetime)
    s = s.replace(hour=0, minute=0, second=0, microsecond=0)
    now = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    days_ago = (s - now).total_seconds() // (24 * 3600)

    x = int(
        ((total_columns + days_ago - 1) * (column_width + 1))
        + left_paddings
        + column_width // 2
    )
    if x < 0 or x >= w:
        return texts

    s = seg["start"]
    assert isinstance(s, datetime)
    y_s = (s.hour * 3600 + s.minute * 60 + s.second) / (24 * 3600)
    s = seg["end"]
    assert isinstance(s, datetime)
    y_e = (s.hour * 3600 + s.minute * 60 + s.second) / (24 * 3600)

    for y in range(int(y_s * h), int(y_e * h)):
        for x_ in range(-(column_width // 2), (column_width // 2) + 1):
            t = "█"
            if color is None or color == "":
                t = f"[{color}]{t}[/]"
            texts[y][x + x_] = t

    return texts


def draw_single(
    texts: list[list[str]],
    d: datetime,
    character: str = "🍐",
    color: str | None = "#ff0000",
    column_width: int = 5,
) -> list[list[str]]:

    assert isinstance(d, datetime)
    assert isinstance(column_width, int), "The column width must be an integer"
    assert column_width % 2 == 1, "The column width needs to be odd"
    assert len(texts) > 0, "The texts must have at least one row"

    w, h = len(texts[0]), len(texts)
    total_columns = w // (column_width + 1)
    left_paddings = (w % (column_width + 1)) // 2

    now = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    days_ago = (d - now).total_seconds() // (24 * 3600)

    x = int(
        ((total_columns + days_ago - 1) * (column_width + 1))
        + left_paddings
        + column_width // 2
    )
    if x < 0 or x >= w:
        return texts

    s = d
    y = (s.hour * 3600 + s.minute * 60 + s.second) / (24 * 3600)

    t = character
    if color is not None and color != "":
        t = f"[{color}]{character}[/]"
    texts[int(y * h)][x] = t

    return texts


def draw_now(texts: list[list[str]], column_width: int = 5) -> list[list[str]]:
    return draw_single(texts, datetime.now(), "N", "#ffffff", column_width)


def show_text(texts: list[list[str]]):
    """Prints the 2D list of strings"""

    # Older and simpler version without optimization
    # for t in texts:
    #     print("".join(t))
    # print(f"Length of all text = {sum([len(''.join(i)) for i in texts])}")

    total = 0
    # color_next = 0
    # current_color = None
    for t in texts:
        too = ""
        for i in t:
            too += i

            # TODO_2: Optimize text printing process (start with a simple list of lists)
            # if i.startswith("[#") and i.endswith("[/]"):
            #     color_next = i[1:8]
            # if current_color == color_next:
            #     p = 0
            #     too = too[:-3] + i[8:]
            # else:
            #     too += i
            # if i.startswith("[#") and i.endswith("[/]"):
            #     current_color = i[1:8]

        total += len(too)
        print(too)
    # print(f"Total length = {total}")


if __name__ == "__main__":
    n = datetime.now()

    t = get_base_text()
    # draw_checkerboard(t)
    draw_columns(t, "|", "#333333", 5)
    draw_time_segment(t, {"start": n - timedelta(hours=2), "end": n})
    draw_single(t, n - timedelta(days=1), "X")
    draw_single(t, n - timedelta(days=2), "X")
    draw_single(t, n - timedelta(days=5), "▲")
    draw_single(t, n - timedelta(days=6), "●")
    draw_single(t, n - timedelta(days=7), "■")
    draw_single(t, n - timedelta(days=57, hours=3), "■")
    draw_now(t)
    show_text(t)
