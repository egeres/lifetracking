import datetime
import math
import os
from typing import Any

from PIL import Image, ImageDraw, ImageFont, ImageOps

from lifetracking.graph.Node import Node
from lifetracking.graph.Time_interval import Time_interval


def draw_arc(
    min_max: tuple[int, int] = (500, 600),
    start: float = 0.0,
    end: float = 1.0,
    color: str = "#FFFFFF",
    img_size: tuple[int, int] = (1500, 1500),
    opacity: float = 1.0,  # TODO_1
) -> Image.Image:
    """Creates an RGBA image of the specified size and with the defined arc

    The arc is antialiased

    min_max indicates the width of the arc, meaning, if it's set to (100, 200), it's
    displaced 100 pixels from the center of the image and it's 100 pixels wide because
    the border is at 200 pixels from the center.

    start and end are the start and end of the arc, in degrees. 0 is 12 o'clock, 90 is
    3 o'clock, 180 is 6 o'clock, 270 is 9 o'clock, and 360 is 12 o'clock again.
    """

    assert 0 <= start <= 1, "Start must be between 0 and 1"
    assert 0 <= end <= 1, "End must be between 0 and 1"
    assert start < end, "Start must be smaller than end"
    assert isinstance(min_max, tuple), "min_max must be a tuple"
    assert isinstance(min_max[0], int), "min_max[0] must be an int"
    assert isinstance(min_max[1], int), "min_max[1] must be an int"

    # Convert the start and end values to degrees
    start_angle = start * 360 - 90
    end_angle = end * 360 - 90

    # Create a new image with 4 times the specified size and transparent background
    image = Image.new("RGBA", (img_size[0] * 4, img_size[1] * 4), (0, 0, 0, 0))

    # Create a draw object
    draw = ImageDraw.Draw(image)

    # Scale up the arc radii by a factor of 4
    min_max = (min_max[0] * 4, min_max[1] * 4)

    # Calculate the bounding boxes for the inner and outer edges of the arc
    bbox_outer = (
        img_size[0] * 2 - min_max[1],
        img_size[1] * 2 - min_max[1],
        img_size[0] * 2 + min_max[1],
        img_size[1] * 2 + min_max[1],
    )
    bbox_inner = (
        img_size[0] * 2 - min_max[0],
        img_size[1] * 2 - min_max[0],
        img_size[0] * 2 + min_max[0],
        img_size[1] * 2 + min_max[0],
    )

    # Draw the outer edge of the arc
    draw.pieslice(bbox_outer, start_angle, end_angle, fill=color)

    # Draw the inner edge of the arc with a transparent color to create the desired arc
    # effect
    draw.pieslice(bbox_inner, start_angle, end_angle, fill=(0, 0, 0, 0))

    # Scale down the image by a factor of 4 with antialiasing
    image = image.resize(img_size, Image.LANCZOS)

    return image


def draw_arc_text(
    text: str,
    radius: int = 500,
    start: float = 0.0,
    end: float = 1.0,
    color: str = "#FF0000",
    img_size: tuple[int, int] = (1500, 1500),
    font_size: float = 50,
    spacing_px: int = 0,
) -> Image.Image:
    """Draws a text in an arc shape and returns an RGBA image of the specified size"""

    assert isinstance(text, str), "Text must be a string"
    assert 0 <= start <= 1, "Start must be between 0 and 1"
    assert 0 <= end <= 1, "End must be between 0 and 1"
    assert start < end, "Start must be smaller than end"

    # create an image and draw an arc
    image = Image.new("RGBA", (img_size[0] * 4, img_size[1] * 4), (0, 0, 0, 0))
    draw = ImageDraw.Draw(image)

    # create a font object
    font_path = r"C:\Github\JetBrainsMono\fonts\ttf\JetBrainsMono-Regular.ttf"
    font = ImageFont.truetype(font_path, int(font_size) * 4)
    text_size = draw.textsize("A", font=font)
    circumference_in_pixels = radius * 4 * math.pi
    text_width = sum([text_size[0] for char in text]) + spacing_px * (len(text) - 1)
    text_angle = text_width / circumference_in_pixels
    start_angle = start + (end - start) / 2 - text_angle / 2

    # draw each character
    for i, char in enumerate(text):
        char_angle = start_angle + i * text_angle / len(text)

        # calculate the character position
        x = (img_size[0] * 4 / 2) + radius * 4 * math.cos(
            math.pi * (char_angle - 0.25) * 2
        )
        y = (img_size[1] * 4 / 2) + radius * 4 * math.sin(
            math.pi * (char_angle - 0.25) * 2
        )

        # draw the rotated character
        char_image = Image.new("RGBA", (font_size * 4, font_size * 4), (0, 0, 0, 0))
        char_draw = ImageDraw.Draw(char_image)
        char_draw.text((0, text_size[1] * -0.15), char, font=font, fill=color)
        rotated_char = char_image.rotate(
            -math.degrees(2 * (char_angle - 0.25) * math.pi) - 90, expand=1
        )
        image.paste(
            rotated_char,
            (int(x - rotated_char.width / 2), int(y - rotated_char.height / 2)),
            mask=rotated_char,
        )

    return image.resize(img_size, Image.LANCZOS)


def make_dailygraph_image(
    pipelines: list[dict[str, Any]],
    # day: datetime.datetime = datetime.datetime.now(),
):
    img = Image.open(rf"C:\Users\{os.getlogin()}\Desktop\Untitled.png")
    img.alpha_composite(draw_arc((499, 501), 0, 1.0, "#FFFFFF"))

    # img.alpha_composite(draw_arc((450, 550), 0.0, 0.5, "#FFFFFF"))
    # img.alpha_composite(draw_arc_text("- SLEEP -", 500, 0.0, 0.5, "#000"))
    # img.show()
    # return

    # FIX: If I specify "today" I cant seem to get "sleep"
    # t
    # start = day.replace(hour=0, minute=0, second=0, microsecond=0)
    # end = day.replace(hour=23, minute=59, second=59, microsecond=999999)
    # t = Time_interval(start, end)
    t = Time_interval.last_n_days(1)

    for i in pipelines:
        graph = i["graph"]
        o = graph.run(t)
        o = [x for x in o if x.start.day == datetime.datetime.now().day]

        for j in o:
            s = j.start.hour / 24 + j.start.minute / 24 / 60
            e = j.end.hour / 24 + j.end.minute / 24 / 60

            s = 0.5
            e = 0.75

            img.alpha_composite(
                draw_arc(
                    (450, 550),
                    s,
                    e,
                    i.get("color", "#FFFFFF"),
                ),
            )

            text = i.get("name", None)
            if text is not None:
                img.alpha_composite(
                    draw_arc_text(
                        text,
                        500,
                        s,
                        e,
                        "#000",
                    )
                )

    img.show()



    return image
