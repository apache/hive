#!/usr/bin/env python3
import csv
import os
import sys
from collections import defaultdict

import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


MODES = ["ERASE", "UPDATE_ONLY", "UPDATE_PLUS_COMPACT"]
MODE_LABELS = {
    "ERASE": "HiveDAE\nERASE",
    "UPDATE_ONLY": "ACID UPDATE\n(no compaction)",
    "UPDATE_PLUS_COMPACT": "ACID UPDATE\n+ major compaction",
}
MODE_COLORS = {
    "ERASE": "#2ca02c",
    "UPDATE_ONLY": "#d62728",
    "UPDATE_PLUS_COMPACT": "#ff7f0e",
}


def load(csv_path):
    rows_real = []
    rows_skip = defaultdict(list)
    with open(csv_path, newline="") as f:
        for r in csv.DictReader(f):
            err = r.get("error", "") or ""
            if err.startswith("skipped:"):
                rows_skip[r["mode"]].append(err[len("skipped:"):].strip())
                continue
            r["subject_count"] = int(r["subject_count"])
            r["table_rows"] = int(r["table_rows"])
            r["t_conform_ms"] = int(r["t_conform_ms"])
            r["bytes_after"] = int(r["bytes_after"])
            r["read_lat_ms"] = int(r["read_lat_ms"])
            r["order_preserved"] = r["order_preserved"] == "true"
            rows_real.append(r)
    return rows_real, rows_skip


def median_per_mode(rows, key):
    buckets = defaultdict(list)
    for r in rows:
        buckets[r["mode"]].append(r[key])
    return {m: float(np.median(vs)) for m, vs in buckets.items()}


def bar_plot(rows_real, rows_skip, key, ylabel, title, fname, outdir,
             log_y=False, cap_for_inf=None):
    med = median_per_mode(rows_real, key)
    fig, ax = plt.subplots(figsize=(6.0, 3.6))
    xs = np.arange(len(MODES))
    ys = []
    skip_idx = []
    for i, m in enumerate(MODES):
        if m in med:
            v = med[m]
            if cap_for_inf is not None and v >= cap_for_inf:
                ys.append(float("nan"))
            else:
                ys.append(v)
        else:
            ys.append(float("nan"))
            skip_idx.append(i)

    finite = [(i, y) for i, y in enumerate(ys) if not np.isnan(y)]
    if finite:
        ax.bar([i for i, _ in finite],
               [y for _, y in finite],
               color=[MODE_COLORS[MODES[i]] for i, _ in finite],
               width=0.55, edgecolor="black")

    if log_y:
        ax.set_ylim(bottom=1)
    ylim = ax.get_ylim()
    text_y = ylim[0] + (ylim[1] - ylim[0]) * 0.04 if not log_y \
        else max(ylim[0] * 1.4, 1.5)
    for i in range(len(MODES)):
        m = MODES[i]
        if np.isnan(ys[i]):
            if m in rows_skip:
                txt = "skipped\n(no ACID)"
            elif cap_for_inf is not None and m in med and med[m] >= cap_for_inf:
                txt = r"$\infty$" + "\n(needs major\ncompaction)"
            else:
                txt = "no data"
            ax.text(i, text_y, txt, ha="center", va="bottom",
                    fontsize=9, color="gray", style="italic")

    ax.set_xticks(xs)
    ax.set_xticklabels([MODE_LABELS[m] for m in MODES], fontsize=9)
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    if log_y:
        ax.set_yscale("log")
    ax.grid(True, axis="y", alpha=0.3)
    fig.tight_layout()
    fig.savefig(os.path.join(outdir, fname))
    plt.close(fig)


def main(argv):
    if len(argv) < 2:
        print('usage: python3 gov_update_compare.py <path-to-update-compare-*.csv> [<output-dir>]')
        sys.exit(1)
    csv_path = argv[1]
    outdir = argv[2] if len(argv) > 2 else os.path.dirname(csv_path) or "."
    os.makedirs(outdir, exist_ok=True)
    real, skip = load(csv_path)

    INF_CAP = 2 ** 60
    bar_plot(real, skip, "t_conform_ms",
             "time to Article-17 conformance (ms)",
             "Time to conformance per mode",
             "gov-update-time.pdf", outdir,
             log_y=True, cap_for_inf=INF_CAP)
    bar_plot(real, skip, "bytes_after",
             "HDFS bytes after deletion command returns",
             "Post-deletion footprint per mode",
             "gov-update-footprint.pdf", outdir,
             log_y=False)
    bar_plot(real, skip, "read_lat_ms",
             "concurrent SELECT latency (ms)",
             "Read latency per mode",
             "gov-update-latency.pdf", outdir,
             log_y=False)

    order = defaultdict(set)
    for r in real:
        order[r["mode"]].add(r["order_preserved"])
    print("order-preserved per mode:")
    for m in MODES:
        if m in order:
            v = "/".join(str(x) for x in sorted(order[m]))
        elif m in skip:
            v = "skipped"
        else:
            v = "n/a"
        print(f"  {m}: {v}")


if __name__ == "__main__":
    main(sys.argv)
