#!/usr/bin/env python3
import csv
import glob
import os
import statistics
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

here = os.path.dirname(os.path.abspath(__file__))
csv_path = sys.argv[1] if len(sys.argv) > 1 else sorted(glob.glob(os.path.join(here, "gov-audit-scale-*.csv")))[-1]
OUTPUT_DIR = ""
out = sys.argv[2] if len(sys.argv) > 2 else os.path.join(OUTPUT_DIR or here, "gov-audit.pdf")

by_size = {}
with open(csv_path) as f:
    for row in csv.DictReader(f):
        by_size.setdefault(int(row["trail_size"]), []).append(float(row["latency_ms"]))

sizes = sorted(by_size)
med = [statistics.median(by_size[s]) for s in sizes]
lo = [med[i] - min(by_size[s]) for i, s in enumerate(sizes)]
hi = [max(by_size[s]) - med[i] for i, s in enumerate(sizes)]

fig, ax = plt.subplots(figsize=(4.2, 2.8))
ax.errorbar(sizes, med, yerr=[lo, hi], fmt="o-", color="#1f77b4",
            capsize=3, lw=1.4, ms=5, label="median (min/max)")
for s, m in zip(sizes, med):
    txt = f"{m:.0f} ms" if m < 1000 else f"{m/1000:.1f} s"
    ax.annotate(txt, (s, m), textcoords="offset points", xytext=(6, -10), fontsize=8)
ax.set_xscale("log")
ax.set_yscale("log")
ax.set_xlabel("audit-trail size (rows)")
ax.set_ylabel("query latency (ms)")
ax.axhline(1000, color="grey", ls=":", lw=0.8)
ax.text(sizes[0], 1100, "1 s", color="grey", fontsize=7, va="bottom")
ax.grid(True, which="both", ls=":", alpha=0.4)
fig.tight_layout()
os.makedirs(os.path.dirname(out), exist_ok=True)
fig.savefig(out, bbox_inches="tight")
print("wrote", os.path.abspath(out))
print("medians:", {s: round(m, 1) for s, m in zip(sizes, med)})
