"""Renders assets/bench-dark.png and assets/bench-light.png from
assets/bench-publish.json, the output of the bench harness run recorded in
docs/readme-trace.md:

    /tmp/bench -target=pulse,jetstream -mode=publish -n=3000 -conc=1,8,32 \
      -warmup=300 -sync=true -pulse-bin=/tmp/pulse-server -dir=/tmp/benchdir \
      -out=assets/bench-publish.json

Run from the repository root: python assets/bench-chart.py
"""
import json
import pathlib

import matplotlib.pyplot as plt

HERE = pathlib.Path(__file__).parent
DATA = json.loads((HERE / "bench-publish.json").read_text())

concs = sorted({row["concurrency"] for row in DATA})
targets = ["pulse", "jetstream"]
colors = {"pulse": "#4f8ff7", "jetstream": "#7c7c7c"}


def rate(target, conc):
    for row in DATA:
        if row["target"] == target and row["concurrency"] == conc:
            return row["msgs_per_sec"]
    raise KeyError(target, conc)


def render(theme, path):
    dark = theme == "dark"
    fg = "#e6e6e6" if dark else "#1a1a1a"
    bg = "#1e1e1e" if dark else "#ffffff"
    grid = "#3a3a3a" if dark else "#dddddd"

    fig, ax = plt.subplots(figsize=(7, 4.2), dpi=150)
    fig.patch.set_facecolor(bg)
    ax.set_facecolor(bg)

    width = 0.35
    x = range(len(concs))
    for i, target in enumerate(targets):
        vals = [rate(target, c) for c in concs]
        offset = (i - 0.5) * width
        ax.bar(
            [xi + offset for xi in x],
            vals,
            width,
            label=target,
            color=colors[target],
        )
        for xi, v in zip(x, vals):
            ax.text(
                xi + offset,
                v + 4,
                f"{v:.0f}",
                ha="center",
                va="bottom",
                fontsize=8,
                color=fg,
            )

    ax.set_xticks(list(x))
    ax.set_xticklabels([f"conc={c}" for c in concs], color=fg)
    ax.set_ylabel("messages / sec", color=fg)
    ax.set_title(
        "Pulse vs NATS JetStream, publish throughput\n"
        "n=3000, 256 B payload, fsync every write, same machine",
        color=fg,
        fontsize=10,
    )
    ax.tick_params(colors=fg)
    ax.grid(axis="y", color=grid, linewidth=0.6)
    for spine in ax.spines.values():
        spine.set_color(grid)
    legend = ax.legend(facecolor=bg, edgecolor=grid, labelcolor=fg)

    fig.tight_layout()
    fig.savefig(path, facecolor=bg)
    plt.close(fig)


render("dark", HERE / "bench-dark.png")
render("light", HERE / "bench-light.png")
print("wrote", HERE / "bench-dark.png", HERE / "bench-light.png")
