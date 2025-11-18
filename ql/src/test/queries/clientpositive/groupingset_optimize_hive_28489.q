-- SORT_QUERY_RESULTS

create table grp_set_test (key string, value string, col0 int, col1 int, col2 int, col3 int);

-- UNION case, can't be optimized
set hive.optimize.grouping.set.threshold=1;
with sub_qr as (select col2 from grp_set_test)
select grpBy_col, sum(col2)
from
( select 'abc' as grpBy_col, col2 from sub_qr union all select 'def' as grpBy_col, col2 from sub_qr) x
group by grpBy_col with rollup;

explain
with sub_qr as (select col2 from grp_set_test)
select grpBy_col, sum(col2)
from
( select 'abc' as grpBy_col, col2 from sub_qr union all select 'def' as grpBy_col, col2 from sub_qr) x
group by grpBy_col with rollup;

insert into grp_set_test values (1, 1, 1, 1, 1, 1), (1, 1, 1, 2, 2, 10), (1, 1, 1, 2, 3, 100);

-- Should not be optimized
set hive.optimize.grouping.set.threshold=-1;
explain
select col0, col1, col2, sum(col3) from grp_set_test group by rollup(col0, col1, col2);

set hive.optimize.grouping.set.threshold=1;
explain
select col0, col1, col2, sum(col3) from (select * from grp_set_test distribute by col0)d group by rollup(col0, col1, col2);

set hive.optimize.grouping.set.threshold=1000000000;
explain
select col0, col1, col2, sum(col3) from grp_set_test group by rollup(col0, col1, col2);
select col0, col1, col2, sum(col3) from grp_set_test group by rollup(col0, col1, col2);

-- Should be optimized
set hive.optimize.grouping.set.threshold=1;
explain
select col0, col1, col2, sum(col3) from grp_set_test group by rollup(col0, col1, col2);
select col0, col1, col2, sum(col3) from grp_set_test group by rollup(col0, col1, col2);

-- Should be optimized, and the selected partition key should not be col3.
alter table grp_set_test update statistics for column col3 set('numDVs'='10000','numNulls'='10000');
explain
select col0, col1, col2, count(distinct col3) from grp_set_test group by rollup(col0, col1, col2);

