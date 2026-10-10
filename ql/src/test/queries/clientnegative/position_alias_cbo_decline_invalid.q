-- HIVE-30037: an out-of-range position is rejected when CBO declines the statement, as it is when CBO plans it
create table pacd_neg (d int);
select d from pacd_neg tablesample (5 rows) order by 2;
