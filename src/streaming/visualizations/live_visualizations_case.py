"""src/streaming/visualizations/live_visualizations_case.py.

Project-specific live visualization functions used by the Kafka consumer.

This module creates a live line chart of sale total by message.
The chart opens in a window while the consumer is running and updates
as each message is consumed.

Author: Denise Case
Date: 2026-05

OBS:
  Don't edit this file - it should remain a working example.
  Copy it, rename it live_visualizations_yourname.py,
  and modify your copy for your own charts.
"""

# === DECLARE IMPORTS ===

from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt

# === DECLARE EXPORTS ===

__all__ = [
    "close_live_chart",
    "init_live_chart",
    "save_live_chart",
    "update_live_chart",
]


# === DEFINE LIVE CHART HELPERS ===


def init_live_chart() -> tuple[Any, Any, list[int], list[float]]:
    """Create and show an empty live chart.

    Returns:
        A tuple of (figure, axis, x_values, y_values).
    """
    plt.ion()

    figure, axis = plt.subplots()
    x_values: list[int] = []
    y_values: list[float] = []

    axis.set_title("Sales Total by Message")
    axis.set_xlabel("Message")
    axis.set_ylabel("Sale Total ($)")

    figure.show()
    figure.canvas.draw()
    figure.canvas.flush_events()

    return figure, axis, x_values, y_values


def update_live_chart(
    *,
    figure: Any,
    axis: Any,
    x_values: list[int],
    y_values: list[float],
    message: dict[str, Any],
) -> None:
    """Update the live chart with one consumed message.

    Arguments:
        figure: Matplotlib figure.
        axis: Matplotlib axis.
        x_values: List of x-axis values already shown.
        y_values: List of y-axis values already shown.
        message: One enriched Kafka message dictionary.

    Returns:
        None.
    """
    x_values.append(int(message["_kafka_offset"]))
    y_values.append(float(message["total"]))

    axis.clear()
    axis.plot(x_values, y_values, marker="o")
    axis.set_title("Sales Total by Message")
    axis.set_xlabel("Message")
    axis.set_ylabel("Sale Total ($)")
    axis.grid(True)

    figure.canvas.draw()
    figure.canvas.flush_events()
    plt.pause(0.05)


def save_live_chart(
    *,
    figure: Any,
    path: Path,
) -> None:
    """Save the final live chart to an image file.

    Arguments:
        figure: Matplotlib figure.
        path: Output image path.

    Returns:
        None.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    figure.savefig(path, bbox_inches="tight")


def close_live_chart() -> None:
    """Turn off interactive chart mode."""
    plt.ioff()
