#!/usr/bin/env python3
import glob
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))

BASE = [
    "microbench-perf-1780557816005.csv",
    "microbench-perf-1780613560508.csv",
    "microbench-perf-1780647228740.csv",
    "microbench-perf-1780682611093.csv",
    "microbench-perf-1780740136604.csv",
]
RERUN_CELLS = {290, 291}
REPS_EXPECTED = 5
IXTYPES_EXPECTED = 7
TOTAL_CELLS = 540

PERREP_HEADER = ("id,num_k,ratio,unique,f_len,ser_fmt,ix_type,time,anon_time,index_time,"
                 "seek_time,run_num,error,total_msg,pii_msg,msg_per_usr,bw,nf,ft")
SUMMARY_HEADER = ("id,num_k,ratio,unique,f_len,ser_fmt,ix_type,reps,proc_med_ms,proc_min_ms,"
                  "proc_max_ms,proc_iqr_ms,anon_med_ms,idx_med_ms,errors,total_msg,pii_msg,"
                  "msg_per_usr,nf,ft")

ID, NUMK, RATIO, UNIQUE, FLEN, FMT, IX = 0, 1, 2, 3, 4, 5, 6
TIME, ANON, INDEX, SEEK, RUNNUM, ERROR = 7, 8, 9, 10, 11, 12
TOTMSG, PIIMSG, MPU, BW, NF, FT = 13, 14, 15, 16, 17, 18


def median(s):
    n = len(s)
    if n == 0:
        return 0
    return s[n // 2] if n % 2 == 1 else (s[n // 2 - 1] + s[n // 2]) // 2


def iqr(s):
    n = len(s)
    if n < 4:
        return 0 if n == 0 else s[-1] - s[0]
    return s[(3 * n) // 4] - s[n // 4]


def read_rows(path):
    rows = []
    with open(path) as fh:
        next(fh)
        for line in fh:
            line = line.rstrip("\n")
            if line:
                rows.append(line.split(","))
    return rows


def cellset(rows):
    return {int(r[ID]) for r in rows}


def load_perrep():
    by_id = {}
    for fn in BASE:
        path = os.path.join(HERE, fn)
        if not os.path.exists(path):
            sys.exit(f"FATAL missing base chunk {fn}")
        for r in read_rows(path):
            by_id.setdefault(int(r[ID]), []).append(r)

    rerun = None
    for path in sorted(glob.glob(os.path.join(HERE, "microbench-perf-1*.csv")),
                       key=os.path.getmtime, reverse=True):
        if os.path.basename(path) in BASE:
            continue
        rows = read_rows(path)
        if rows and cellset(rows) == RERUN_CELLS:
            rerun = (path, rows)
            break
    if rerun:
        path, rows = rerun
        for cid in RERUN_CELLS:
            by_id[cid] = []
        for r in rows:
            by_id[int(r[ID])].append(r)
        print(f"  dedup: cells {sorted(RERUN_CELLS)} replaced from {os.path.basename(path)}")
    else:
        print(f"  dedup: NO re-run found for {sorted(RERUN_CELLS)} (still using BASE rows)")
    return by_id, bool(rerun)


def summarize_cell(rows):
    groups = {}
    for r in rows:
        groups.setdefault(r[IX], []).append(r)
    out = []
    for ix, g in groups.items():
        proc = sorted(int(r[TIME]) for r in g)
        anon = sorted(int(r[ANON]) for r in g)
        idx = sorted(int(r[INDEX]) for r in g)
        errs = sum(int(r[ERROR]) for r in g)
        f = g[0]
        out.append((ix, len(g), ",".join([
            f[ID], f[NUMK], f[RATIO], f[UNIQUE], f[FLEN], f[FMT], ix,
            str(len(g)), str(median(proc)), str(proc[0]), str(proc[-1]), str(iqr(proc)),
            str(median(anon)), str(median(idx)), str(errs),
            f[TOTMSG], f[PIIMSG], f[MPU], f[NF], f[FT],
        ])))
    return out


def main():
    by_id, have_rerun = load_perrep()
    ids = sorted(by_id)
    print(f"  cells present: {len(ids)} / {TOTAL_CELLS}")

    gaps = [c for c in range(1, TOTAL_CELLS + 1) if c not in by_id]
    if gaps:
        print(f"  gaps ({len(gaps)}): {gaps[:10]}{'...' if len(gaps) > 10 else ''}")

    bad, total_err, perrep_n, summ_n = [], 0, 0, 0
    for cid in ids:
        rows = by_id[cid]
        perrep_n += len(rows)
        ixg = {}
        for r in rows:
            ixg.setdefault(r[IX], 0)
            ixg[r[IX]] += 1
            total_err += int(r[ERROR])
        if len(ixg) != IXTYPES_EXPECTED or any(v != REPS_EXPECTED for v in ixg.values()):
            bad.append((cid, dict(ixg)))
        summ_n += len(ixg)
    if bad:
        print(f"  WARN {len(bad)} cells not {IXTYPES_EXPECTED}x{REPS_EXPECTED}: {bad[:5]}")
    print(f"  per-rep rows={perrep_n} (expect {TOTAL_CELLS*IXTYPES_EXPECTED*REPS_EXPECTED})"
          f"  summary rows={summ_n} (expect {TOTAL_CELLS*IXTYPES_EXPECTED})  errors={total_err}")

    complete = (len(ids) == TOTAL_CELLS and not gaps and not bad and have_rerun)
    if not complete:
        print("  PARTIAL / unverified -> NOT writing canonical (dry-run mechanics OK)")
        return 1

    perrep_path = os.path.join(HERE, "microbench-perf-canonical-540.csv")
    summ_path = os.path.join(HERE, "microbench-perf-summary-canonical-540.csv")
    with open(perrep_path, "w") as pr, open(summ_path, "w") as su:
        pr.write(PERREP_HEADER + "\n")
        su.write(SUMMARY_HEADER + "\n")
        for cid in ids:
            for r in by_id[cid]:
                pr.write(",".join(r) + "\n")
            for _ix, _n, line in summarize_cell(by_id[cid]):
                su.write(line + "\n")
    print(f"  WROTE {os.path.basename(perrep_path)} + {os.path.basename(summ_path)}")
    return 0


if __name__ == "__main__":
    print("merge_chunks: stitching §6.2 chunks ->")
    sys.exit(main())
