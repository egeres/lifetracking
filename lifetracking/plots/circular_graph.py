from __future__ import annotations

from PIL import Image, ImageDraw


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
    return image.resize(img_size, Image.LANCZOS)
