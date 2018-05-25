set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
;

set hive.exec.reducers.max = 1;

-- SORT_QUERY_RESULTS

CREATE TABLE tbl1_n10(key int, value string) CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS;
CREATE TABLE tbl2_n9(key int, value string) CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS;

insert overwrite table tbl1_n10
select * from src where key < 10;

insert overwrite table tbl2_n9
select * from src where key < 10;

set hive.auto.convert.join=true;
set hive.optimize.bucketmapjoin = true;
set hive.optimize.bucketmapjoin.sortedmerge = true;
set hive.auto.convert.sortmerge.join=true;
set hive.auto.convert.sortmerge.join.to.mapjoin=false;
--disable hash joins
set hive.auto.convert.join.noconditionaltask.size=1;

-- The join is being performed as part of sub-query. It should be converted to a sort-merge join
explain
select count(*) from (
  select a.key as key, a.value as val1, b.value as val2 from tbl1_n10 a join tbl2_n9 b on a.key = b.key
) subq1;

select count(*) from (
  select a.key as key, a.value as val1, b.value as val2 from tbl1_n10 a join tbl2_n9 b on a.key = b.key
) subq1;

-- The join is being performed as part of sub-query. It should be converted to a sort-merge join
explain
select key, count(*) from 
(
  select a.key as key, a.value as val1, b.value as val2 from tbl1_n10 a join tbl2_n9 b on a.key = b.key
) subq1
group by key;

select key, count(*) from 
(
  select a.key as key, a.value as val1, b.value as val2 from tbl1_n10 a join tbl2_n9 b on a.key = b.key
) subq1
group by key;

-- The join is being performed as part of more than one sub-query. It should be converted to a sort-merge join
explain
select count(*) from
(
  select key, count(*) from 
  (
    select a.key as key, a.value as val1, b.value as val2 from tbl1_n10 a join tbl2_n9 b on a.key = b.key
  ) subq1
  group by key
) subq2;

select count(*) from
(
  select key, count(*) from 
  (
    select a.key as key, a.value as val1, b.value as val2 from tbl1_n10 a join tbl2_n9 b on a.key = b.key
  ) subq1
  group by key
) subq2;

-- A join is being performed across different sub-queries, where a join is being performed in each of them.
-- Each sub-query should be converted to a sort-merge join.
explain
select src1.key, src1.cnt1, src2.cnt1 from
(
  select key, count(*) as cnt1 from 
  (
    select a.key as key, a.value as val1, b.value as val2 from tbl1_n10 a join tbl2_n9 b on a.key = b.key
  ) subq1 group by key
) src1
join
(
  select key, count(*) as cnt1 from 
  (
    select a.key as key, a.value as val1, b.value as val2 from tbl1_n10 a join tbl2_n9 b on a.key = b.key
  ) subq2 group by key
) src2
on src1.key = src2.key;

select src1.key, src1.cnt1, src2.cnt1 from
(
  select key, count(*) as cnt1 from 
  (
    select a.key as key, a.value as val1, b.value as val2 from tbl1_n10 a join tbl2_n9 b on a.key = b.key
  ) subq1 group by key
) src1
join
(
  select key, count(*) as cnt1 from 
  (
    select a.key as key, a.value as val1, b.value as val2 from tbl1_n10 a join tbl2_n9 b on a.key = b.key
  ) subq2 group by key
) src2
on src1.key = src2.key;

-- The subquery itself is being joined. Since the sub-query only contains selects and filters, it should 
-- be converted to a sort-merge join.
explain
select count(*) from 
  (select a.key as key, a.value as value from tbl1_n10 a where key < 6) subq1 
    join
  (select a.key as key, a.value as value from tbl2_n9 a where key < 6) subq2
  on subq1.key = subq2.key;

select count(*) from 
  (select a.key as key, a.value as value from tbl1_n10 a where key < 6) subq1 
    join
  (select a.key as key, a.value as value from tbl2_n9 a where key < 6) subq2
  on subq1.key = subq2.key;

-- The subquery itself is being joined. Since the sub-query only contains selects and filters, it should 
-- be converted to a sort-merge join, although there is more than one level of sub-query
explain
select count(*) from 
  (
  select * from
    (
      select a.key as key, a.value as value from tbl1_n10 a where key < 8
    ) subq1 
  where key < 6
  ) subq2
  join tbl2_n9 b
  on subq2.key = b.key;

select count(*) from 
  (
  select * from
    (
      select a.key as key, a.value as value from tbl1_n10 a where key < 8
    ) subq1 
  where key < 6
  ) subq2
  join tbl2_n9 b
  on subq2.key = b.key;

-- Both the tables are nested sub-queries i.e more then 1 level of sub-query.
-- The join should be converted to a sort-merge join
explain
select count(*) from 
  (
  select * from
    (
      select a.key as key, a.value as value from tbl1_n10 a where key < 8
    ) subq1 
  where key < 6
  ) subq2
  join
  (
  select * from
    (
      select a.key as key, a.value as value from tbl1_n10 a where key < 8
    ) subq3 
  where key < 6
  ) subq4
  on subq2.key = subq4.key;

select count(*) from 
  (
  select * from
    (
      select a.key as key, a.value as value from tbl1_n10 a where key < 8
    ) subq1 
  where key < 6
  ) subq2
  join
  (
  select * from
    (
      select a.key as key, a.value as value from tbl1_n10 a where key < 8
    ) subq3 
  where key < 6
  ) subq4
  on subq2.key = subq4.key;

-- The subquery itself is being joined. Since the sub-query only contains selects and filters and the join key
-- is not getting modified, it should be converted to a sort-merge join. Note that the sub-query modifies one 
-- item, but that is not part of the join key.
explain
select count(*) from 
  (select a.key as key, concat(a.value, a.value) as value from tbl1_n10 a where key < 8) subq1 
    join
  (select a.key as key, concat(a.value, a.value) as value from tbl2_n9 a where key < 8) subq2
  on subq1.key = subq2.key;

select count(*) from 
  (select a.key as key, concat(a.value, a.value) as value from tbl1_n10 a where key < 8) subq1 
    join
  (select a.key as key, concat(a.value, a.value) as value from tbl2_n9 a where key < 8) subq2
  on subq1.key = subq2.key;

-- Since the join key is modified by the sub-query, neither sort-merge join not bucketized mapside
-- join should be performed
explain
select count(*) from 
  (select a.key +1 as key, concat(a.value, a.value) as value from tbl1_n10 a) subq1 
    join
  (select a.key +1 as key, concat(a.value, a.value) as value from tbl2_n9 a) subq2
  on subq1.key = subq2.key;

select count(*) from 
  (select a.key +1 as key, concat(a.value, a.value) as value from tbl1_n10 a) subq1 
    join
  (select a.key +1 as key, concat(a.value, a.value) as value from tbl2_n9 a) subq2
  on subq1.key = subq2.key;

-- The left table is a sub-query and the right table is not.
-- It should be converted to a sort-merge join.
explain
select count(*) from 
  (select a.key as key, a.value as value from tbl1_n10 a where key < 6) subq1 
    join tbl2_n9 a on subq1.key = a.key;

select count(*) from 
  (select a.key as key, a.value as value from tbl1_n10 a where key < 6) subq1 
    join tbl2_n9 a on subq1.key = a.key;

-- The right table is a sub-query and the left table is not.
-- It should be converted to a sort-merge join.
explain
select count(*) from tbl1_n10 a
  join
  (select a.key as key, a.value as value from tbl2_n9 a where key < 6) subq1 
  on a.key = subq1.key;

select count(*) from tbl1_n10 a
  join
  (select a.key as key, a.value as value from tbl2_n9 a where key < 6) subq1 
  on a.key = subq1.key;

-- There are more than 2 inputs to the join, all of them being sub-queries. 
-- It should be converted to to a sort-merge join
explain
select count(*) from 
  (select a.key as key, a.value as value from tbl1_n10 a where key < 6) subq1 
    join
  (select a.key as key, a.value as value from tbl2_n9 a where key < 6) subq2
  on (subq1.key = subq2.key)
    join
  (select a.key as key, a.value as value from tbl2_n9 a where key < 6) subq3
  on (subq1.key = subq3.key);

select count(*) from 
  (select a.key as key, a.value as value from tbl1_n10 a where key < 6) subq1 
    join
  (select a.key as key, a.value as value from tbl2_n9 a where key < 6) subq2
  on subq1.key = subq2.key
    join
  (select a.key as key, a.value as value from tbl2_n9 a where key < 6) subq3
  on (subq1.key = subq3.key);

-- The join is being performed on a nested sub-query, and an aggregation is performed after that.
-- The join should be converted to a sort-merge join
explain
select count(*) from (
  select subq2.key as key, subq2.value as value1, b.value as value2 from
  (
    select * from
    (
      select a.key as key, a.value as value from tbl1_n10 a where key < 8
    ) subq1
    where key < 6
  ) subq2
join tbl2_n9 b
on subq2.key = b.key) a;

select count(*) from (
  select subq2.key as key, subq2.value as value1, b.value as value2 from
  (
    select * from
    (
      select a.key as key, a.value as value from tbl1_n10 a where key < 8
    ) subq1
    where key < 6
  ) subq2
join tbl2_n9 b
on subq2.key = b.key) a;

set hive.auto.convert.sortmerge.join.to.mapjoin=true;

-- The join is being performed as part of sub-query. It should be converted to a sort-merge join
explain
select count(*) from (
  select a.key as key, a.value as val1, b.value as val2 from tbl1_n10 a join tbl2_n9 b on a.key = b.key
) subq1;

select count(*) from (
  select a.key as key, a.value as val1, b.value as val2 from tbl1_n10 a join tbl2_n9 b on a.key = b.key
) subq1;

-- The join is being performed as part of sub-query. It should be converted to a sort-merge join
explain
select key, count(*) from 
(
  select a.key as key, a.value as val1, b.value as val2 from tbl1_n10 a join tbl2_n9 b on a.key = b.key
) subq1
group by key;

select key, count(*) from 
(
  select a.key as key, a.value as val1, b.value as val2 from tbl1_n10 a join tbl2_n9 b on a.key = b.key
) subq1
group by key;

-- The join is being performed as part of more than one sub-query. It should be converted to a sort-merge join
explain
select count(*) from
(
  select key, count(*) from 
  (
    select a.key as key, a.value as val1, b.value as val2 from tbl1_n10 a join tbl2_n9 b on a.key = b.key
  ) subq1
  group by key
) subq2;

select count(*) from
(
  select key, count(*) from 
  (
    select a.key as key, a.value as val1, b.value as val2 from tbl1_n10 a join tbl2_n9 b on a.key = b.key
  ) subq1
  group by key
) subq2;

-- A join is being performed across different sub-queries, where a join is being performed in each of them.
-- Each sub-query should be converted to a sort-merge join.
explain
select src1.key, src1.cnt1, src2.cnt1 from
(
  select key, count(*) as cnt1 from 
  (
    select a.key as key, a.value as val1, b.value as val2 from tbl1_n10 a join tbl2_n9 b on a.key = b.key
  ) subq1 group by key
) src1
join
(
  select key, count(*) as cnt1 from 
  (
    select a.key as key, a.value as val1, b.value as val2 from tbl1_n10 a join tbl2_n9 b on a.key = b.key
  ) subq2 group by key
) src2
on src1.key = src2.key;

select src1.key, src1.cnt1, src2.cnt1 from
(
  select key, count(*) as cnt1 from 
  (
    select a.key as key, a.value as val1, b.value as val2 from tbl1_n10 a join tbl2_n9 b on a.key = b.key
  ) subq1 group by key
) src1
join
(
  select key, count(*) as cnt1 from 
  (
    select a.key as key, a.value as val1, b.value as val2 from tbl1_n10 a join tbl2_n9 b on a.key = b.key
  ) subq2 group by key
) src2
on src1.key = src2.key;

-- The subquery itself is being joined. Since the sub-query only contains selects and filters, it should 
-- be converted to a sort-merge join.
explain
select count(*) from 
  (select a.key as key, a.value as value from tbl1_n10 a where key < 6) subq1 
    join
  (select a.key as key, a.value as value from tbl2_n9 a where key < 6) subq2
  on subq1.key = subq2.key;

select count(*) from 
  (select a.key as key, a.value as value from tbl1_n10 a where key < 6) subq1 
    join
  (select a.key as key, a.value as value from tbl2_n9 a where key < 6) subq2
  on subq1.key = subq2.key;

-- The subquery itself is being joined. Since the sub-query only contains selects and filters, it should 
-- be converted to a sort-merge join, although there is more than one level of sub-query
explain
select count(*) from 
  (
  select * from
    (
      select a.key as key, a.value as value from tbl1_n10 a where key < 8
    ) subq1 
  where key < 6
  ) subq2
  join tbl2_n9 b
  on subq2.key = b.key;

select count(*) from 
  (
  select * from
    (
      select a.key as key, a.value as value from tbl1_n10 a where key < 8
    ) subq1 
  where key < 6
  ) subq2
  join tbl2_n9 b
  on subq2.key = b.key;

-- Both the tables are nested sub-queries i.e more then 1 level of sub-query.
-- The join should be converted to a sort-merge join
explain
select count(*) from 
  (
  select * from
    (
      select a.key as key, a.value as value from tbl1_n10 a where key < 8
    ) subq1 
  where key < 6
  ) subq2
  join
  (
  select * from
    (
      select a.key as key, a.value as value from tbl1_n10 a where key < 8
    ) subq3 
  where key < 6
  ) subq4
  on subq2.key = subq4.key;

select count(*) from 
  (
  select * from
    (
      select a.key as key, a.value as value from tbl1_n10 a where key < 8
    ) subq1 
  where key < 6
  ) subq2
  join
  (
  select * from
    (
      select a.key as key, a.value as value from tbl1_n10 a where key < 8
    ) subq3 
  where key < 6
  ) subq4
  on subq2.key = subq4.key;

-- The subquery itself is being joined. Since the sub-query only contains selects and filters and the join key
-- is not getting modified, it should be converted to a sort-merge join. Note that the sub-query modifies one 
-- item, but that is not part of the join key.
explain
select count(*) from 
  (select a.key as key, concat(a.value, a.value) as value from tbl1_n10 a where key < 8) subq1 
    join
  (select a.key as key, concat(a.value, a.value) as value from tbl2_n9 a where key < 8) subq2
  on subq1.key = subq2.key;

select count(*) from 
  (select a.key as key, concat(a.value, a.value) as value from tbl1_n10 a where key < 8) subq1 
    join
  (select a.key as key, concat(a.value, a.value) as value from tbl2_n9 a where key < 8) subq2
  on subq1.key = subq2.key;

-- The left table is a sub-query and the right table is not.
-- It should be converted to a sort-merge join.
explain
select count(*) from 
  (select a.key as key, a.value as value from tbl1_n10 a where key < 6) subq1 
    join tbl2_n9 a on subq1.key = a.key;

select count(*) from 
  (select a.key as key, a.value as value from tbl1_n10 a where key < 6) subq1 
    join tbl2_n9 a on subq1.key = a.key;

-- The right table is a sub-query and the left table is not.
-- It should be converted to a sort-merge join.
explain
select count(*) from tbl1_n10 a
  join
  (select a.key as key, a.value as value from tbl2_n9 a where key < 6) subq1 
  on a.key = subq1.key;

select count(*) from tbl1_n10 a
  join
  (select a.key as key, a.value as value from tbl2_n9 a where key < 6) subq1 
  on a.key = subq1.key;

-- There are more than 2 inputs to the join, all of them being sub-queries. 
-- It should be converted to to a sort-merge join
explain
select count(*) from 
  (select a.key as key, a.value as value from tbl1_n10 a where key < 6) subq1 
    join
  (select a.key as key, a.value as value from tbl2_n9 a where key < 6) subq2
  on (subq1.key = subq2.key)
    join
  (select a.key as key, a.value as value from tbl2_n9 a where key < 6) subq3
  on (subq1.key = subq3.key);

select count(*) from 
  (select a.key as key, a.value as value from tbl1_n10 a where key < 6) subq1 
    join
  (select a.key as key, a.value as value from tbl2_n9 a where key < 6) subq2
  on subq1.key = subq2.key
    join
  (select a.key as key, a.value as value from tbl2_n9 a where key < 6) subq3
  on (subq1.key = subq3.key);

-- The join is being performed on a nested sub-query, and an aggregation is performed after that.
-- The join should be converted to a sort-merge join
explain
select count(*) from (
  select subq2.key as key, subq2.value as value1, b.value as value2 from
  (
    select * from
    (
      select a.key as key, a.value as value from tbl1_n10 a where key < 8
    ) subq1
    where key < 6
  ) subq2
join tbl2_n9 b
on subq2.key = b.key) a;

select count(*) from (
  select subq2.key as key, subq2.value as value1, b.value as value2 from
  (
    select * from
    (
      select a.key as key, a.value as value from tbl1_n10 a where key < 8
    ) subq1
    where key < 6
  ) subq2
join tbl2_n9 b
on subq2.key = b.key) a;
