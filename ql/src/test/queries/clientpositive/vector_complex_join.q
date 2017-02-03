set hive.cli.print.header=true;
set hive.explain.user=false;
set hive.fetch.task.conversion=none;
SET hive.vectorized.execution.enabled=true;
SET hive.auto.convert.join=true;
set hive.fetch.task.conversion=none;

-- From HIVE-10729.  Not expected to vectorize this query.
--
CREATE TABLE test (a INT, b MAP<INT, STRING>) STORED AS ORC;
INSERT OVERWRITE TABLE test SELECT 199408978, MAP(1, "val_1", 2, "val_2") FROM src LIMIT 1;

explain vectorization expression
select * from alltypesorc join test where alltypesorc.cint=test.a;

select * from alltypesorc join test where alltypesorc.cint=test.a;



CREATE TABLE test2a (a ARRAY<INT>) STORED AS ORC;
INSERT OVERWRITE TABLE test2a SELECT ARRAY(1, 2) FROM src LIMIT 1;

CREATE TABLE test2b (a INT) STORED AS ORC;
INSERT OVERWRITE TABLE test2b VALUES (2), (3), (4);

explain vectorization expression
select *  from test2b join test2a on test2b.a = test2a.a[1];

select *  from test2b join test2a on test2b.a = test2a.a[1];