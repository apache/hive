#!/usr/bin/env python3
import csv
import os
import sys
from collections import defaultdict

import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


def load(csv_path):
    rows = []
    int_cols = {"id", "S", "R", "P", "time_us", "run_num", "error",
                "c1_count", "c2_count", "c3_count",
                "rules_emitted", "resolved_rules", "rows_inserted", "bytes"}
    flt_cols = {"conflict_density"}
    with open(csv_path, newline="") as f:
        for r in csv.DictReader(f):
            for k in int_cols:
                if k in r:
                    r[k] = int(r[k])
            for k in flt_cols:
                if k in r:
                    r[k] = float(r[k])
            r["time_ms"] = r["time_us"] / 1000.0
            r["sr"] = r["S"] * r["R"]
            rows.append(r)
    return rows


def median(xs):
    return float(np.median(xs)) if xs else float("nan")


def plot_validate(rows, out):
    sub = [r for r in rows if r["runner"] == "V" and r["error"] == 0]
    if not sub:
        print("V: no rows, skipping"); return
    keyed = defaultdict(list)
    for r in sub:
        keyed[(r["sr"], r["mode"])].append(r["time_ms"])
    modes = sorted({k[1] for k in keyed})
    fig, ax = plt.subplots(figsize=(6.0, 3.4))
    for m in modes:
        xs = sorted({k[0] for k in keyed if k[1] == m})
        ys = [median(keyed[(x, m)]) for x in xs]
        ax.plot(xs, ys, marker="o", label=m)
    ax.set_xlabel("Rules per policy (S × R)")
    ax.set_ylabel("Validate latency (ms)")
    ax.set_xscale("log", base=2)
    ax.set_yscale("log")
    ax.grid(True, which="both", alpha=0.3)
    ax.legend(title="Resolution mode")
    fig.tight_layout()
    fig.savefig(os.path.join(out, "gov-validate.pdf"))
    plt.close(fig)
    print(f"V: {len(sub)} rows -> gov-validate.pdf")


def plot_detect(rows, out):
    sub = [r for r in rows if r["runner"] == "D" and r["error"] == 0]
    if not sub:
        print("D: no rows, skipping"); return
    keyed = defaultdict(list)
    c1s = defaultdict(list)
    for r in sub:
        keyed[(r["P"], r["conflict_density"])].append(r["time_ms"])
        c1s[(r["P"], r["conflict_density"])].append(r["c1_count"])
    p_vals = sorted({k[0] for k in keyed})
    cd_vals = sorted({k[1] for k in keyed})
    grid = np.full((len(p_vals), len(cd_vals)), np.nan)
    annot = np.empty_like(grid, dtype=object)
    for (p, cd), vs in keyed.items():
        i = p_vals.index(p); j = cd_vals.index(cd)
        t = median(vs)
        grid[i, j] = t
        annot[i, j] = f"{t:.1f}\nc1={int(median(c1s[(p, cd)]))}"
    fig, ax = plt.subplots(figsize=(5.4, 0.6 + 0.7 * len(p_vals)))
    im = ax.imshow(grid, aspect="auto", cmap="YlOrRd")
    ax.set_xticks(range(len(cd_vals))); ax.set_xticklabels([f"{c:.2f}" for c in cd_vals])
    ax.set_yticks(range(len(p_vals)));  ax.set_yticklabels([str(p) for p in p_vals])
    ax.set_xlabel("Conflict density"); ax.set_ylabel("Policies per binding (P)")
    for i in range(len(p_vals)):
        for j in range(len(cd_vals)):
            if annot[i, j] is not None:
                ax.text(j, i, annot[i, j], ha="center", va="center", fontsize=7)
    fig.colorbar(im, ax=ax, label="Detect latency (ms)")
    fig.tight_layout()
    fig.savefig(os.path.join(out, "gov-detect.pdf"))
    plt.close(fig)
    print(f"D: {len(sub)} rows -> gov-detect.pdf")


def plot_attach(rows, out):
    sub = [r for r in rows if r["runner"] == "A" and r["error"] == 0]
    if not sub:
        print("A: no rows, skipping"); return
    keyed = defaultdict(list)
    for r in sub:
        keyed[(r["sr"], r["P"])].append(r["time_ms"])
    p_vals = sorted({k[1] for k in keyed})
    fig, ax = plt.subplots(figsize=(6.0, 3.4))
    for p_val in p_vals:
        xs = sorted({k[0] for k in keyed if k[1] == p_val})
        ys = [median(keyed[(x, p_val)]) for x in xs]
        ax.plot(xs, ys, marker="s", label=f"P={p_val}")
    ax.set_xlabel("Rules per policy (S × R)")
    ax.set_ylabel("Attach latency (ms)")
    ax.set_xscale("log", base=2)
    ax.grid(True, which="both", alpha=0.3)
    ax.legend()
    fig.tight_layout()
    fig.savefig(os.path.join(out, "gov-attach.pdf"))
    plt.close(fig)
    print(f"A: {len(sub)} rows -> gov-attach.pdf")


def plot_audit(rows, out):
    sub = [r for r in rows if r["runner"] == "U" and r["error"] == 0]
    if not sub:
        print("U: no rows, skipping"); return
    keyed = defaultdict(list)
    for r in sub:
        keyed[r["rows_inserted"]].append(r["time_ms"])
    xs = sorted(keyed)
    med = [median(keyed[x]) for x in xs]
    lo  = [min(keyed[x]) for x in xs]
    hi  = [max(keyed[x]) for x in xs]
    fig, ax = plt.subplots(figsize=(6.0, 3.4))
    ax.errorbar(xs, med,
                yerr=[np.array(med) - np.array(lo), np.array(hi) - np.array(med)],
                fmt="o-", capsize=4)
    ax.set_xlabel("Audit trail rows returned")
    ax.set_ylabel("Audit query latency (ms)")
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(os.path.join(out, "gov-audit.pdf"))
    plt.close(fig)
    print(f"U: {len(sub)} rows -> gov-audit.pdf")


def main():
    if len(sys.argv) < 2:
        print('usage: python3 gov_plot.py <path-to-gov-stats-*.csv> [<output-dir>]'); sys.exit(2)
    csv_path = sys.argv[1]
    out_dir = sys.argv[2] if len(sys.argv) > 2 else os.path.dirname(csv_path) or "."
    os.makedirs(out_dir, exist_ok=True)
    rows = load(csv_path)
    print(f"loaded {len(rows)} rows from {csv_path}")
    plot_validate(rows, out_dir)
    plot_detect(rows, out_dir)
    plot_attach(rows, out_dir)
    plot_audit(rows, out_dir)


if __name__ == "__main__":
    main()
