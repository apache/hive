create table orc_src (key string, value string) STORED AS ORC;
insert into table orc_src select * from src;

set hive.execution.engine=tez;
set hive.vectorized.execution.enabled=true;
set hive.auto.convert.join.noconditionaltask.size=1;
set hive.exec.reducers.bytes.per.reducer=20000;

explain
SELECT count(*) FROM src, orc_src where src.key=orc_src.key;

SELECT count(*) FROM src, orc_src where src.key=orc_src.key;
