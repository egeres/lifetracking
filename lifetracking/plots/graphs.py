from __future__ import annotations

import datetime
import dis
import hashlib
import inspect
import json
import os
import pickle
import tempfile
from typing import Any, Callable

import pandas as pd
import plotly.graph_objects as go
from dateutil.parser import parse

from lifetracking.graph.Time_interval import Time_interval


def graph_udate_layout(
    fig: go.Figure,
    t: Time_interval | None,
):
    # Base
    newlayout = {
        "template": "plotly_dark",
        "margin": dict(l=0, r=0, t=0, b=0),  # Is this a good idea tho?
        "legend": {
            "font": {
                "family": "JetBrains Mono",
            }
        },
    }

    days_span = None
    if (
        len(fig.data) > 0
        and len(fig.data[0].x) > 0
        and isinstance(fig.data[0].x[0], (datetime.datetime, datetime.date))
    ):
        days_span = (max(fig.data[0].x) - min(fig.data[0].x)).days
    elif t is not None:
        days_span = (t.end - t.start).days

    # x ticks
    if days_span is None:
        pass
    elif days_span > 50:
        # Month display
        fig.update_xaxes(
            tickformat="%b",
            dtick="M1",
            ticklabelmode="period",
            ticks="outside",
        )
    elif days_span > 15:
        # Week display (starting on monday)
        first_date = min(fig.data[0].x)
        first_monday = first_date - pd.DateOffset(days=(first_date.weekday()))
        fig.update_xaxes(
            tickformat="%Y-%m-%d",
            dtick="604800000",  # one week in milliseconds
            tick0=first_monday,
            ticks="outside",
        )
    else:
        # Day display
        fig.update_xaxes(
            tickformat="%d",
            dtick="86400000",  # one day in milliseconds
            ticks="outside",
        )

    if len(fig.data) == 1:
        # Hide legend
        newlayout["showlegend"] = False
    elif len(fig.data) > 1 and (
        fig.layout.showlegend is not False or fig.layout.showlegend is None
    ):
        newlayout["margin"]["r"] = 250

    fig.update_layout(**newlayout)


def graph_annotate_today(
    fig: go.Figure,
    t: Time_interval,
    minmax: tuple[float, float] | None = None,
):
    # Min max
    # fig_min, fig_max = (0, 1) if minmax is None else (min(0, minmax[0]), minmax[1])
    fig_min, fig_max = (0, 1) if minmax is None else (minmax[0], minmax[1])

    # Today
    today = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    # TODO: At some point, decide upon tzinfo's
    if t.start.tzinfo is not None:
        today = today.astimezone(t.start.tzinfo)

    # Plot or not
    days_diff = (today - t.start).days
    if days_diff >= 0 and days_diff <= int(t.duration_days):
        fig.add_shape(
            type="line",
            x0=today,
            x1=today,
            y0=fig_min,
            y1=fig_max,
            line={"color": "#a00"},
            xref="x",
            yref="y",
        )


def graph_annotate_annotations(
    fig: go.Figure,
    t: Time_interval,
    annotations: list | None,
    minmax: tuple[float, float] | None = None,
):
    assert isinstance(fig, go.Figure)
    if annotations is None:
        return

    fig_min, fig_max = (0, 1) if minmax is None else (minmax[0], minmax[1])
    y_offset_of_10_percent = (fig_max - fig_min) * 0.1

    for i in annotations:
        # Days diff
        date = i["date"]
        if isinstance(date, str):
            date = parse(date)
        assert isinstance(date, datetime.datetime)
        if t.start.tzinfo is not None:
            date = date.astimezone(t.start.tzinfo)
        days_diff = (date - t.start).days

        if not (days_diff >= 0 and days_diff <= int(t.duration_days)):
            continue

        # fig_min, fig_max = (0, 1) if minmax is None else
        # (min(0, minmax[0]), minmax[1])

        fig.add_shape(
            type="line",
            x0=date,
            x1=date,
            y0=fig_min,
            y1=fig_max,
            line={
                "color": i.get("color", "#aaa"),
                "dash": "dash",
                "width": 1,
            },
            xref="x",
            yref="y",
        )
        if "title" in i:
            fig.add_annotation(
                x=date,
                y=fig_max - y_offset_of_10_percent,
                text=i["title"],
                showarrow=False,
                xref="x",
                yref="y",
                yshift=10,
            )


def graph_annotate_title(
    fig: go.Figure,
    title: str | None,
):
    """Write a lable on the top left corner"""

    assert isinstance(fig, go.Figure)
    assert isinstance(title, str) or title is None

    # css = """
    # %%html
    # <style>
    # @font-face {
    #     font-family: 'JetBrains Mono';
    #     src: url(
    # 'C:/Github/lifetracking_pipelines/Assets/JetBrainsMono-Regular.ttf'
    # ) format('truetype');
    # }
    # </style>
    # """
    # display(HTML(css))

    if title is None:
        return fig

    fig.add_annotation(
        xref="paper",
        yref="paper",
        x=0.05,
        y=0.9,
        text=title,
        showarrow=False,
        font=dict(
            family="JetBrains Mono",
            size=16,
        ),
    )
    return fig
