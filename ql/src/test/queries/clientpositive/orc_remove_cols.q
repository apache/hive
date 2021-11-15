--! qt:dataset:alltypesorc

set hive.vectorized.execution.enabled=false;
SET hive.exec.schema.evolution=false;
set hive.fetch.task.conversion=more;
set hive.mapred.mode=nonstrict;

CREATE TABLE orc_partitioned(a INT, b STRING) partitioned by (ds string) STORED AS ORC;
insert into table orc_partitioned partition (ds = 'today') select cint, cstring1 from alltypesorc where cint is not null order by cint limit 10;
insert into table orc_partitioned partition (ds = 'tomorrow') select cint, cstring1 from alltypesorc where cint is not null order by cint limit 10;

-- Use the old change the SERDE trick to avoid ORC DDL checks... and remove a column on the end.
ALTER TABLE orc_partitioned SET SERDE 'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe';
ALTER TABLE orc_partitioned REPLACE COLUMNS (a int);
ALTER TABLE orc_partitioned SET SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde';

SELECT * FROM orc_partitioned WHERE ds = 'today';
SELECT * FROM orc_partitioned WHERE ds = 'tomorrow';

