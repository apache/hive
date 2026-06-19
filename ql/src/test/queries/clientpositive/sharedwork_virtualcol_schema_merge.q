set hive.optimize.shared.work.merge.ts.schema=false;

create table t1(a int);

-- 3 map vertices scans table t1
explain
WITH t AS (
  select BLOCK__OFFSET__INSIDE__FILE, INPUT__FILE__NAME, a from (
    select BLOCK__OFFSET__INSIDE__FILE, INPUT__FILE__NAME, a, row_number() OVER (partition by INPUT__FILE__NAME) rn from t1
    where a = 1
  ) q
  where rn=1
)
select BLOCK__OFFSET__INSIDE__FILE, INPUT__FILE__NAME, a from t1 where NOT (a = 1) AND INPUT__FILE__NAME IN (select INPUT__FILE__NAME from t)
union all
select * from t;



set hive.optimize.shared.work.merge.ts.schema=true;

-- 2 of 3 map vertices scanning table t1 are merged:
-- One projects BLOCK__OFFSET__INSIDE__FILE and INPUT__FILE__NAME and the second one projects INPUT__FILE__NAME only.
-- These are merged to one scan which projects both.
explain
WITH t AS (
  select BLOCK__OFFSET__INSIDE__FILE, INPUT__FILE__NAME, a from (
    select BLOCK__OFFSET__INSIDE__FILE, INPUT__FILE__NAME, a, row_number() OVER (partition by INPUT__FILE__NAME) rn from t1
    where a = 1
  ) q
  where rn=1
)
select BLOCK__OFFSET__INSIDE__FILE, INPUT__FILE__NAME, a from t1 where NOT (a = 1) AND INPUT__FILE__NAME IN (select INPUT__FILE__NAME from t)
union all
select * from t;
