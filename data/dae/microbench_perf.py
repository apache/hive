#!/usr/bin/env python3
import csv
import glob
import os
import sys
from collections import defaultdict

import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


FMT_ORDER = ["J", "M", "X", "P", "A"]
FMT_LABELS = {"J": "JSON", "M": "MsgPack", "X": "XML",
              "P": "Protobuf", "A": "Avro"}

PROC_GROUPS = ["BTREE", "DIR", "TAB", "NOIX"]
PROC_LABELS = {"BTREE": "b-tree", "DIR": "directory",
               "TAB": "tabular", "NOIX": "no index"}
PROC_COLORS = {"BTREE": "#1f77b4", "DIR": "#2ca02c",
               "TAB": "#9467bd", "NOIX": "#d62728"}

OUTPUT_DIR = ""


def load(csv_path):
    rows = []
    with open(csv_path, newline="") as f:
        for r in csv.DictReader(f):
            for k in ("proc_med_ms", "proc_iqr_ms", "idx_med_ms",
                      "anon_med_ms", "reps", "errors"):
                r[k] = int(r[k])
            rows.append(r)
    return rows


def proc_group(ix_type):
    if ix_type.startswith("B"):
        return "BTREE"
    return {"L": "DIR", "T": "TAB", "N": "NOIX"}.get(ix_type)


def formats_present(rows):
    seen = {r["ser_fmt"] for r in rows}
    return [f for f in FMT_ORDER if f in seen] + sorted(seen - set(FMT_ORDER))


def proc_figure(rows, outdir):
    fmts = formats_present(rows)
    med = defaultdict(lambda: None)
    iqr = {}
    for r in rows:
        g = proc_group(r["ix_type"])
        if g is None:
            continue
        key = (r["ser_fmt"], g)
        if med[key] is None or r["proc_med_ms"] < med[key]:
            med[key] = r["proc_med_ms"]
            iqr[key] = r["proc_iqr_ms"]

    fig, ax = plt.subplots(figsize=(6.4, 3.6))
    x = np.arange(len(fmts))
    width = 0.2
    for gi, g in enumerate(PROC_GROUPS):
        ys = [med[(f, g)] if med[(f, g)] is not None else np.nan for f in fmts]
        es = [iqr.get((f, g), 0) for f in fmts]
        off = (gi - (len(PROC_GROUPS) - 1) / 2) * width
        ax.bar(x + off, ys, width, yerr=es, capsize=2,
               color=PROC_COLORS[g], edgecolor="black", linewidth=0.4,
               label=PROC_LABELS[g], error_kw={"lw": 0.8})

    ax.set_xticks(x)
    ax.set_xticklabels([FMT_LABELS.get(f, f) for f in fmts])
    ax.set_ylabel("ERASE processing time (ms)")
    ax.set_title(r"Single-node ERASE per format ($10^7$ msgs, all-to-PII 100)")
    ax.grid(True, axis="y", ls=":", alpha=0.4)
    ax.legend(fontsize=8, ncol=4, loc="upper center",
              bbox_to_anchor=(0.5, -0.13), frameon=False)
    fig.tight_layout()
    path = os.path.join(outdir, "microbench-proc.pdf")
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)

    speed = {}
    for f in fmts:
        idx_best = min(v for (ff, g), v in med.items()
                       if ff == f and g != "NOIX" and v is not None)
        n = med.get((f, "NOIX"))
        if n:
            speed[f] = n / idx_best
    return path, speed


def index_figure(rows, outdir):
    order = ["B1", "B2", "B3", "B4", "L", "T"]
    label = {"B1": "b-tree\np1", "B2": "b-tree\np2", "B3": "b-tree\np3",
             "B4": "b-tree\np4", "L": "directory", "T": "tabular"}
    by_ix = defaultdict(list)
    for r in rows:
        if r["ix_type"] in order:
            by_ix[r["ix_type"]].append(r["idx_med_ms"])

    strat = [s for s in order if s in by_ix]
    mean = [float(np.mean(by_ix[s])) for s in strat]
    lo = [mean[i] - min(by_ix[s]) for i, s in enumerate(strat)]
    hi = [max(by_ix[s]) - mean[i] for i, s in enumerate(strat)]
    colors = ["#1f77b4" if s.startswith("B") else
              ("#2ca02c" if s == "L" else "#9467bd") for s in strat]

    fig, ax = plt.subplots(figsize=(5.6, 3.4))
    xs = np.arange(len(strat))
    ax.bar(xs, mean, yerr=[lo, hi], capsize=3, color=colors,
           edgecolor="black", linewidth=0.4, width=0.6,
           error_kw={"lw": 0.8})
    for i, m in enumerate(mean):
        ax.annotate(f"{m:.0f}", (xs[i], m), textcoords="offset points",
                    xytext=(0, 4), ha="center", fontsize=8)
    ax.set_xticks(xs)
    ax.set_xticklabels([label[s] for s in strat], fontsize=8)
    ax.set_ylabel("index lookup: build + seek (ms)")
    ax.set_title("Index lookup cost by strategy (mean over formats)")
    ax.grid(True, axis="y", ls=":", alpha=0.4)
    fig.tight_layout()
    path = os.path.join(outdir, "microbench-index.pdf")
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)
    return path, {s: round(float(np.mean(by_ix[s])), 1) for s in strat}


def main(argv):
    here = os.path.dirname(os.path.abspath(__file__))
    if len(argv) > 1:
        csv_path = argv[1]
    else:
        hits = sorted(glob.glob(os.path.join(here, "microbench-perf-summary-*.csv")))
        if not hits:
            print("no microbench-perf-summary-*.csv found; pass one explicitly")
            sys.exit(1)
        csv_path = hits[-1]
    outdir = argv[2] if len(argv) > 2 else (OUTPUT_DIR or os.path.dirname(csv_path) or here)
    os.makedirs(outdir, exist_ok=True)

    rows = load(csv_path)
    p1, speed = proc_figure(rows, outdir)
    p2, idx = index_figure(rows, outdir)

    print("read", os.path.abspath(csv_path), f"({len(rows)} cells)")
    print("wrote", os.path.abspath(p1))
    print("wrote", os.path.abspath(p2))
    print("no-index / best-indexed speedup per format:",
          {FMT_LABELS.get(f, f): round(s, 2) for f, s in speed.items()})
    print("index lookup median ms by strategy:", idx)


if __name__ == "__main__":
    main(sys.argv)
