set hive.mapred.mode=nonstrict;
SET hive.vectorized.execution.enabled=true;
set hive.exec.dynamic.partition.mode=nonstrict;

SET hive.llap.io.enabled=false;

SET hive.exec.orc.default.buffer.size=32768;
SET hive.exec.orc.default.row.index.stride=1000;
SET hive.optimize.index.filter=true;
set hive.auto.convert.join=false;

DROP TABLE llap_stats;

CREATE TABLE llap_stats(ctinyint TINYINT, csmallint SMALLINT) partitioned by (cint int) STORED AS ORC;

insert into table llap_stats partition(cint)
select cint, ctinyint, csmallint from alltypesorc where cint is not null limit 10;

select * from llap_stats;

SET hive.llap.io.enabled=true;

explain analyze table llap_stats partition (cint) compute statistics for columns;
analyze table llap_stats partition (cint) compute statistics for columns;

DROP TABLE llap_stats;
