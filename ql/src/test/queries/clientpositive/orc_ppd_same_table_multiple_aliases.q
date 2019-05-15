-- SORT_QUERY_RESULTS;

set hive.vectorized.execution.enabled=false;
set hive.optimize.index.filter=true;
create table test_table_n6(number int) stored as ORC;

-- Two insertions will create two files, with one stripe each
insert into table test_table_n6 VALUES (1);
insert into table test_table_n6 VALUES (2);

-- This should return 2 records
select * from test_table_n6;

-- These should each return 1 record
select * from test_table_n6 where number = 1;
select * from test_table_n6 where number = 2;

-- This should return 2 records
select * from test_table_n6 where number = 1 union all select * from test_table_n6 where number = 2;
