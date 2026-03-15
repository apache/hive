CREATE TABLE test_stats0 (e decimal(38,10)) stored as orc;
insert into test_stats0 (e) values (0.0);

set hive.vectorized.execution.enabled=false;
select count(*) from test_stats0 where CAST(e as DECIMAL(15,1)) BETWEEN 100.0 AND 1000.0;


set hive.vectorized.execution.enabled=true;
select count(*) from test_stats0 where CAST(e as DECIMAL(15,1)) BETWEEN 100.0 AND 1000.0;
