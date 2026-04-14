CREATE TABLE test_stats0 (e decimal(38,10)) stored as orc;
insert into test_stats0 (e) values (0.0);

set hive.vectorized.execution.enabled=false;
select count(*) from test_stats0 where CAST(e as DECIMAL(15,1)) BETWEEN 100.0 AND 1000.0;

set hive.vectorized.execution.enabled=true;
EXPLAIN VECTORIZATION DETAIL select count(*) from test_stats0 where CAST(e as DECIMAL(15,1)) BETWEEN 100.0 AND 1000.0;
select count(*) from test_stats0 where CAST(e as DECIMAL(15,1)) BETWEEN 100.0 AND 1000.0;

EXPLAIN VECTORIZATION DETAIL select count(*) from test_stats0 where CAST(e as DECIMAL(30,1)) BETWEEN 100.0 AND 1000.0;
select count(*) from test_stats0 where CAST(e as DECIMAL(30,1)) BETWEEN 100.0 AND 1000.0;


CREATE TABLE test_stats1 (int_col INT) stored as orc;
insert into test_stats1 (int_col) values (0);

set hive.vectorized.execution.enabled=false;
select count(*) from test_stats1 where CAST(int_col as DECIMAL(15,1)) BETWEEN 100.0 AND 1000.0;

set hive.vectorized.execution.enabled=true;
EXPLAIN VECTORIZATION DETAIL select count(*) from test_stats1 where CAST(int_col as DECIMAL(15,1)) BETWEEN 100.0 AND 1000.0;
select count(*) from test_stats1 where CAST(int_col as DECIMAL(15,1)) BETWEEN 100.0 AND 1000.0;

EXPLAIN VECTORIZATION DETAIL select count(*) from test_stats1 where CAST(int_col as DECIMAL(30,1)) BETWEEN 100.0 AND 1000.0;
select count(*) from test_stats1 where CAST(int_col as DECIMAL(30,1)) BETWEEN 100.0 AND 1000.0;