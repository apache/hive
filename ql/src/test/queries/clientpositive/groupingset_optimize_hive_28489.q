create table grp_set_test (key string, value string, col0 int, col1 int, col2 int, col3 int);

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

-- Should be optimized
set hive.optimize.grouping.set.threshold=1;
explain
select col0, col1, col2, sum(col3) from (select * from grp_set_test distribute by col0)d group by rollup(col0, col1, col2);
