--! qt:dataset:src
--! qt:dataset:alltypesorc
set hive.cli.print.header=true;
set hive.explain.user=false;
set hive.fetch.task.conversion=none;
SET hive.vectorized.execution.enabled=true;
SET hive.auto.convert.join=true;
set hive.fetch.task.conversion=none;

-- The test_n8 case is updated for HIVE-18043.
--
CREATE TABLE test_n8 (a INT, b MAP<INT, STRING>) STORED AS ORC;
INSERT OVERWRITE TABLE test_n8 SELECT 199408978, MAP(1, "val_1", 2, "val_2") FROM src LIMIT 1;

explain vectorization expression
select * from alltypesorc join test_n8 where alltypesorc.cint=test_n8.a;

select * from alltypesorc join test_n8 where alltypesorc.cint=test_n8.a;



CREATE TABLE test2a_n0 (a ARRAY<INT>, index INT) STORED AS ORC;
INSERT OVERWRITE TABLE test2a_n0 SELECT ARRAY(1, 2), 1 FROM src LIMIT 1;

CREATE TABLE test2b_n0 (a INT) STORED AS ORC;
INSERT OVERWRITE TABLE test2b_n0 VALUES (2), (3), (4);

explain vectorization expression
select *  from test2b_n0 join test2a_n0 on test2b_n0.a = test2a_n0.a[1];

select *  from test2b_n0 join test2a_n0 on test2b_n0.a = test2a_n0.a[1];

explain vectorization expression
select *  from test2b_n0 join test2a_n0 on test2b_n0.a = test2a_n0.a[test2a_n0.index];

select *  from test2b_n0 join test2a_n0 on test2b_n0.a = test2a_n0.a[test2a_n0.index];