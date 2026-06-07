#!/usr/bin/env python3
import csv
import os
import sys
from collections import defaultdict
import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
CSV = sys.argv[1] if len(sys.argv) > 1 else os.path.join(HERE, "microbench-perf-canonical-540.csv")

FMT = {"J": "JSON", "M": "MsgPack", "X": "XML", "P": "Protobuf", "A": "Avro"}
BTREE = ["B1", "B2", "B3", "B4"]
PAGE = {"B1": "128KB", "B2": "256KB", "B3": "512KB", "B4": "1024KB"}
CFG_AXES = ("ser_fmt", "nf", "num_k", "unique", "ratio", "f_len")


def load():
    acc = defaultdict(lambda: {"bw": [], "time": [], "anon": [], "idx": []})
    for r in csv.DictReader(open(CSV)):
        k = (r["ser_fmt"], r["nf"], r["num_k"], r["unique"], r["ratio"], r["f_len"], r["ix_type"])
        a = acc[k]
        a["bw"].append(float(r["bw"]))
        a["time"].append(int(r["time"]))
        a["anon"].append(int(r["anon_time"]))
        a["idx"].append(int(r["index_time"]))
    cell = {}
    for k, a in acc.items():
        cell[k] = {"bw": float(np.mean(a["bw"])), "time": float(np.median(a["time"])),
                   "anon": float(np.median(a["anon"])), "idx": float(np.median(a["idx"]))}
    return cell


def configs(cell):
    cfg = defaultdict(dict)
    for k, v in cell.items():
        cfg[k[:6]][k[6]] = v
    return cfg


def main():
    cell = load()
    cfg = configs(cell)
    ixs = sorted({k[6] for k in cell})
    print(f"loaded {CSV.split('/')[-1]}: {len(cell)} (cfg,ix) cells, {len(cfg)} configs, ix={ixs}\n")

    print("=== 1. throughput (bw, M msg/s) per index type, over all configs ===")
    perix = {ix: np.array([cfg[c][ix]["bw"] for c in cfg if ix in cfg[c]]) for ix in ixs}
    for ix in sorted(ixs, key=lambda i: -perix[i].mean()):
        a = perix[ix]
        print(f"  {ix:3s} mean={a.mean():.3f}  median={np.median(a):.3f}  min={a.min():.3f}  max={a.max():.3f}")

    print("\n=== 2. no-index (N) dominance & best-indexed/N speedup ===")
    n_lowest = 0
    speed = []
    speed_by_kf = defaultdict(list)
    for c in cfg:
        d = cfg[c]
        if "N" not in d:
            continue
        bw_n = d["N"]["bw"]
        idx_best = max(v["bw"] for ix, v in d.items() if ix != "N")
        if bw_n <= min(v["bw"] for ix, v in d.items() if ix != "N"):
            n_lowest += 1
        speed.append(idx_best / bw_n)
        speed_by_kf[(c[2], c[1])].append(idx_best / bw_n)
    speed = np.array(speed)
    print(f"  N is the strict-lowest-throughput column in {n_lowest}/{len(cfg)} configs")
    print(f"  best-indexed/N speedup: min={speed.min():.2f} median={np.median(speed):.2f} "
          f"mean={speed.mean():.2f} max={speed.max():.2f}")
    print("  speedup by (K, FN):")
    for (k, f) in sorted(speed_by_kf, key=lambda t: (int(t[0]), int(t[1]))):
        a = np.array(speed_by_kf[(k, f)])
        print(f"    K={k:>3s} FN={f:>2s}: mean={a.mean():.2f}  range {a.min():.2f}-{a.max():.2f}")

    print("\n=== 3. b-tree page size (throughput) ===")
    bt = {b: perix[b].mean() for b in BTREE}
    for b in BTREE:
        print(f"  {b} ({PAGE[b]:>6s}): mean bw={bt[b]:.3f}")
    mono = all(bt[BTREE[i]] >= bt[BTREE[i + 1]] for i in range(3))
    print(f"  monotone B1>=B2>=B3>=B4? {mono};  B1 vs B4 spread = {100*(bt['B1']-bt['B4'])/bt['B1']:.1f}%")

    print("\n=== 4. per-config winner (argmax throughput) ===")
    wins = defaultdict(int)
    for c in cfg:
        w = max(cfg[c], key=lambda ix: cfg[c][ix]["bw"])
        wins[w] += 1
    for ix in sorted(wins, key=lambda i: -wins[i]):
        print(f"  {ix:3s}: {wins[ix]:3d} wins  ({100*wins[ix]/len(cfg):.0f}%)")

    print("\n=== 5. sequential (max L,T) vs best b-tree (max B1..B4) ===")
    seq_win = 0
    n = 0
    for c in cfg:
        d = cfg[c]
        if not all(x in d for x in ["L", "T"] + BTREE):
            continue
        n += 1
        seq = max(d["L"]["bw"], d["T"]["bw"])
        bbt = max(d[b]["bw"] for b in BTREE)
        if seq >= bbt:
            seq_win += 1
    print(f"  sequential >= best b-tree in {seq_win}/{n} configs ({100*seq_win/n:.0f}%)")

    print("\n=== 6. directory (L) vs tabular (T) by field length (steadiness) ===")
    for fl in ["10", "20", "30"]:
        L = np.array([cfg[c]["L"]["bw"] for c in cfg if c[5] == fl and "L" in cfg[c]])
        T = np.array([cfg[c]["T"]["bw"] for c in cfg if c[5] == fl and "T" in cfg[c]])
        print(f"  FL={fl}: L mean={L.mean():.3f}  T mean={T.mean():.3f}  (L-T)/L={100*(L.mean()-T.mean())/L.mean():+.1f}%")

    print("\n=== 7. format spread within each (ix, non-format cfg) group ===")
    fmt_groups = defaultdict(dict)
    for k, v in cell.items():
        fmt_groups[(k[6], k[1], k[2], k[3], k[4], k[5])][k[0]] = v["bw"]
    spreads = []
    for g, fm in fmt_groups.items():
        vals = np.array(list(fm.values()))
        if len(vals) >= 2 and vals.mean() > 0:
            spreads.append(100 * (vals.max() - vals.min()) / vals.mean())
    spreads = np.array(spreads)
    print(f"  (max-min)/mean across formats: median={np.median(spreads):.1f}%  mean={spreads.mean():.1f}%  "
          f"p90={np.percentile(spreads,90):.1f}%  max={spreads.max():.1f}%")
    print("  overall throughput per format (mean over all ix+configs):")
    perfmt = defaultdict(list)
    for k, v in cell.items():
        perfmt[k[0]].append(v["bw"])
    for f in sorted(perfmt, key=lambda f: -np.mean(perfmt[f])):
        print(f"    {FMT[f]:8s}: mean bw={np.mean(perfmt[f]):.3f}")

    print("\n=== 8. index lookup cost idx_med_ms per ix (format-independent) ===")
    for ix in ["L", "T"] + BTREE:
        a = np.array([cfg[c][ix]["idx"] for c in cfg if ix in cfg[c]])
        tag = PAGE.get(ix, "")
        print(f"  {ix:3s} {tag:>6s}: median={np.median(a):.0f}ms  mean={a.mean():.0f}ms")

    print("\n=== headline: proc time no-index vs best-indexed (ms) ===")
    tn = np.array([cfg[c]["N"]["time"] for c in cfg if "N" in cfg[c]])
    tb = np.array([min(v["time"] for ix, v in cfg[c].items() if ix != "N") for c in cfg if "N" in cfg[c]])
    print(f"  N median time={np.median(tn):.0f}ms  best-indexed median time={np.median(tb):.0f}ms  "
          f"time ratio median={np.median(tn/tb):.2f}x")


if __name__ == "__main__":
    main()
