--! qt:replace:/(\s+Statistics: Num rows: \d+)/#Masked#/
set hive.vectorized.execution.enabled=true;
set hive.explain.user=false;

drop table if exists dec64_parquet;

create table dec64_parquet (k int, d decimal(7,2)) stored as parquet;
insert into dec64_parquet values
  (1, 1.10), (1, 2.20), (2, 3.30), (2, 4.40), (3, 5.50), (3, cast(null as decimal(7,2)));

-- Verifies that the Parquet vectorized reader engages the DECIMAL_64 path:
explain vectorization detail
select k, sum(d) from dec64_parquet group by k;

select k, sum(d) from dec64_parquet group by k order by k;