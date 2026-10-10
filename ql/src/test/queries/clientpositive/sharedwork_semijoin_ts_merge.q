--! qt:dataset:
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.auto.convert.join=false;
set hive.tez.dynamic.semijoin.reduction=true;
set hive.tez.bigtable.minsize.semijoin.reduction=1;
set hive.tez.min.bloom.filter.entries=1;
set hive.tez.dynamic.semijoin.reduction.threshold=-1;
set hive.optimize.shared.work=true;
set hive.optimize.shared.work.semijoin=false;

create table fact_n1 (k bigint, v int);
create table dim_n1 (k bigint, x int);
insert into fact_n1 values (1, 10), (1, 20), (2, 30), (3, 40), (4, 1);
insert into dim_n1 values (1, 1), (2, 1), (3, 2), (4, 1);
analyze table fact_n1 compute statistics for columns;
analyze table dim_n1 compute statistics for columns;

-- Both scans of fact_n1 carry a semijoin filter from identical dim_n1 branches, but their
-- filters differ, so the shared work optimizer merges only the scans and keeps one semijoin
-- branch: the discarded scan's filter must not keep referencing the removed branch
explain
select a.s, b.s from
  (select sum(f.v) s from fact_n1 f join dim_n1 d on f.k = d.k where d.x = 1) a,
  (select sum(f.v) s from fact_n1 f join dim_n1 d on f.k = d.k where d.x = 1 and f.v > 5) b;

select a.s, b.s from
  (select sum(f.v) s from fact_n1 f join dim_n1 d on f.k = d.k where d.x = 1) a,
  (select sum(f.v) s from fact_n1 f join dim_n1 d on f.k = d.k where d.x = 1 and f.v > 5) b;
