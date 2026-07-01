#!/usr/bin/env python3
import csv
import os
import sys
from collections import defaultdict

import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

HERE = os.path.dirname(os.path.abspath(__file__))
CSV = os.path.join(HERE, "microbench-perf-canonical-540.csv")
OUTPUT_DIR = ""

FORMATS = ["A", "J", "M", "P", "X"]
IX = ["B1", "B2", "B3", "B4", "L", "N", "T"]
TOTAL = 10_000_000
F_LENS = [10, 20, 30]
RATIOS = [10, 100, 1000]
UNIQUES = [1000, 10000]
KS = [1, 10, 100]
FNS = [1, 10]


def load():
    acc = defaultdict(list)
    for r in csv.DictReader(open(CSV)):
        key = (r["ser_fmt"], int(r["nf"]), int(r["num_k"]), int(r["unique"]),
               int(r["ratio"]), int(r["f_len"]), r["ix_type"])
        acc[key].append(float(r["bw"]))
    return {k: float(np.mean(v)) for k, v in acc.items()}


def panel_matrix(cell, nf, k, unique, ratio, f_len):
    m = np.full((len(FORMATS), len(IX)), np.nan)
    for fi, f in enumerate(FORMATS):
        for xi, ix in enumerate(IX):
            v = cell.get((f, nf, k, unique, ratio, f_len, ix))
            if v is not None:
                m[fi, xi] = v
    return m


def draw(cell, k, fn, outdir):
    vmax = max((v for (f, nf, kk, u, r, fl, ix), v in cell.items() if kk == k and nf == fn),
               default=1.0)
    vmax = 1.0 if vmax < 1 else vmax
    vmin = 0.2

    fig, axn = plt.subplots(6, 3, sharex=False, sharey=True, figsize=(8, 11))
    cbar_ax = fig.add_axes((0.93, 0.3, 0.02, 0.4))
    im = None
    panels = [(u, r, fl) for u in UNIQUES for r in RATIOS for fl in F_LENS]
    for idx, ax in enumerate(axn.flat):
        u, r, fl = panels[idx]
        mat = panel_matrix(cell, fn, k, u, r, fl)
        im = ax.imshow(mat, cmap="jet", vmin=vmin, vmax=vmax, aspect="auto")
        ax.set_xticks(range(len(IX)))
        ax.set_yticks(range(len(FORMATS)))
        ax.set_xticklabels(IX, fontsize=6)
        ax.set_yticklabels(FORMATS, fontsize=6)
        ax.set_title(f"FL:{fl}, PII:{TOTAL // r:,}, UU:{u:,}", fontsize=8)
        for fi in range(len(FORMATS)):
            for xi in range(len(IX)):
                val = mat[fi, xi]
                if not np.isnan(val):
                    rel = (val - vmin) / (vmax - vmin + 1e-9)
                    ax.text(xi, fi, f"{val:.2f}", ha="center", va="center",
                            fontsize=5.2, color="white" if rel < 0.5 else "black")
        ax.set_xticks(np.arange(-0.5, len(IX), 1), minor=True)
        ax.set_yticks(np.arange(-0.5, len(FORMATS), 1), minor=True)
        ax.grid(which="minor", color="black", linewidth=0.5)
        ax.tick_params(which="minor", length=0)
        ax.tick_params(axis="both", labelsize=6)
        ax.set_ylabel("serialization format" if idx % 3 == 0 else "", fontsize=6)
        ax.set_xlabel("index type" if idx // 3 >= 5 else "", fontsize=6)

    cb = fig.colorbar(im, cax=cbar_ax)
    cb.set_label("throughput (M msg/s)", size=7)
    cb.ax.tick_params(labelsize=6)
    fig.suptitle(f"Num Searched Users: {k}, Num Files: {fn}, Total messages: {TOTAL:,}",
                 fontsize=8, fontweight="bold")
    plt.subplots_adjust(left=0.05, right=0.90, top=0.95, bottom=0.05, wspace=0.09, hspace=0.30)

    name = ("fig-FLD_LEN=[10, 20, 30]-RATIO=[10, 100, 1000]-UU=[1000, 10000]"
            f"-K={k}-FN={fn}.png")
    path = os.path.join(outdir, name)
    fig.savefig(path, dpi=300)
    plt.close(fig)
    return path, vmax


def main(argv):
    outdir = argv[1] if len(argv) > 1 else (OUTPUT_DIR or HERE)
    os.makedirs(outdir, exist_ok=True)
    cell = load()
    print(f"read {os.path.basename(CSV)} -> {len(cell)} (fmt,cfg,ix) cells; outdir={outdir}")
    for k in KS:
        for fn in FNS:
            path, vmax = draw(cell, k, fn, outdir)
            print(f"  wrote K={k:>3d} FN={fn:>2d}  vmax={vmax:5.2f}  {os.path.basename(path)}")


if __name__ == "__main__":
    main(sys.argv)
