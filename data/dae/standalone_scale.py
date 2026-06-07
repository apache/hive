#!/usr/bin/env python3
import csv
import os
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


def load(csv_path):
    rows = []
    with open(csv_path, newline="") as f:
        for r in csv.DictReader(f):
            rows.append({
                "batch_size":       int(r["batch_size"]),
                "wall_time_ms":     int(r["wall_time_ms"]),
                "files_rewritten":  int(r["files_rewritten"]),
                "messages_per_file": float(r["messages_per_file"]),
            })
    rows.sort(key=lambda r: r["batch_size"])
    return rows


def main(argv):
    if len(argv) < 2:
        print('usage: python3 standalone_scale.py <path-to-standalone-scale-*.csv> [<output-dir>]')
        sys.exit(1)
    csv_path = argv[1]
    outdir = argv[2] if len(argv) > 2 else os.path.dirname(csv_path) or "."
    os.makedirs(outdir, exist_ok=True)
    rows = load(csv_path)
    xs    = [r["batch_size"]        for r in rows]
    wall  = [r["wall_time_ms"]      for r in rows]
    files = [r["files_rewritten"]   for r in rows]
    mpf   = [r["messages_per_file"] for r in rows]

    fig, (ax_left, ax_right) = plt.subplots(1, 2, figsize=(9.6, 3.5))

    ax_left.set_xscale("log")
    ax_left.set_xlabel("batch size $B$")
    ax_left.set_ylabel("wall time (ms)", color="#2a6fbb")
    line_wt = ax_left.plot(xs, wall, marker="o", linewidth=2,
                           color="#2a6fbb", label="wall time")
    ax_left.tick_params(axis="y", labelcolor="#2a6fbb")
    ax_left.set_ylim(bottom=0)
    ax_left.grid(True, which="both", alpha=0.25)

    ax_files = ax_left.twinx()
    ax_files.set_ylabel("files rewritten", color="#cc6600")
    bar_widths = [0.45 * x for x in xs]
    bars_f = ax_files.bar(xs, files, width=bar_widths, alpha=0.35,
                          color="#cc6600", edgecolor="#cc6600",
                          label="files rewritten")
    ax_files.tick_params(axis="y", labelcolor="#cc6600")
    ax_files.set_ylim(0, max(files) * 1.25 + 1)
    ax_left.set_title("Wall time vs batch size")

    ax_right.set_xscale("log")
    ax_right.set_yscale("log")
    ax_right.set_xlabel("batch size $B$")
    ax_right.set_ylabel("messages per touched file")
    ax_right.plot(xs, mpf, marker="o", linewidth=2, color="#1f7a3f")
    ax_right.grid(True, which="both", alpha=0.25)
    ax_right.set_title("Per-file message density")

    fig.tight_layout()
    out = os.path.join(outdir, "standalone-scale.pdf")
    fig.savefig(out)
    plt.close(fig)
    print(f"wrote {out}")


if __name__ == "__main__":
    main(sys.argv)
