--! qt:dataset:src
--! qt:dataset:src1

set hive.auto.convert.join=true;
set hive.llap.mapjoin.memory.oversubscribe.factor=0;
set hive.auto.convert.join.noconditionaltask.size=500;

-- The InMemoryDataSize of MapJoin is 280. Therefore, SWO should not merge 2 TSs reading src
-- as the sum of InMemoryDataSize of 2 unmerged MapJoin exceeds 500.
-- TSs are identical and FILs are not identical.
explain extended
with
a as (
  select src.key a, src.value b, src1.value c
  from src, src1
  where src.key = src1.key and src.value > 1000000 and src1.value > 1000000
),
b as (
  select src.key a, src.value b, src1.value c
  from src, src1
  where src.key = src1.key and src.value > 1000001 and src1.value > 1000001
),
aa as (
  select a, avg(b) as b, sum(c) as c from a group by a
),
bb as (
  select a, avg(b) as b, sum(c) as c from b group by a
)
select * from aa join bb on aa.a = bb.a;


-- The InMemoryDataSize of MapJoin is 280. Since the limit is 1000, SWO should not merge 4 TSs into a single TS.
-- TSs are identical and TS.getNumChild() > 1.
set hive.auto.convert.join.noconditionaltask.size=1000;
explain extended
with
a as (
  select src.key a, src.value b, src1.value c
  from src, src1
  where src.key = src1.key and src.value > 1000000 and src1.value > 1000000
),
b as (
  select src.key a, 2 * src.value b, src1.value c
  from src, src1
  where src.key = src1.key and src.value > 1000001 and src1.value > 1000001
),
c as (
  select src.key a, 3 * src.value b, src1.value c
  from src, src1
  where src.key = src1.key and src.value > 1000002 and src1.value > 1000002
),
d as (
  select src.key a, 4 * src.value b, src1.value c
  from src, src1
  where src.key = src1.key and src.value > 1000003 and src1.value > 1000003
),
aa as (
  select a, avg(b) as b, sum(c) as c from a group by a
),
bb as (
  select a, avg(b) as b, sum(c) as c from b group by a
),
cc as (
  select a, avg(b) as b, sum(c) as c from c group by a
),
dd as (
  select a, avg(b) as b, sum(c) as c from d group by a
)
select * from aa join bb join cc join dd on aa.a = bb.a and aa.a = cc.a and aa.a = dd.a;

-- The InMemoryDataSize of MapJoin is 280. Since the limit is 1000, SWO should not merge 4 TSs into a single TS.
-- TSs, FILs are identical and FIL.getNumChild() > 1.
set hive.auto.convert.join.noconditionaltask.size=1000;
explain extended
with
a as (
  select src.key a, src.value b, src1.value c
  from src, src1
  where src.key = src1.key and src.value > 1000000 and src1.value > 1000000
),
b as (
  select src.key a, 2 * src.value b, src1.value c
  from src, src1
  where src.key = src1.key and src.value > 1000000 and src1.value > 1000001
),
c as (
  select src.key a, 3 * src.value b, src1.value c
  from src, src1
  where src.key = src1.key and src.value > 1000000 and src1.value > 1000002
),
d as (
  select src.key a, 4 * src.value b, src1.value c
  from src, src1
  where src.key = src1.key and src.value > 1000000 and src1.value > 1000003
),
aa as (
  select a, avg(b) as b, sum(c) as c from a group by a
),
bb as (
  select a, avg(b) as b, sum(c) as c from b group by a
),
cc as (
  select a, avg(b) as b, sum(c) as c from c group by a
),
dd as (
  select a, avg(b) as b, sum(c) as c from d group by a
)
select * from aa join bb join cc join dd on aa.a = bb.a and aa.a = cc.a and aa.a = dd.a;
