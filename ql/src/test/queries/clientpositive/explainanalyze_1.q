set hive.map.aggr=false;
set hive.mapred.mode=nonstrict;

explain analyze select * from src a union all select * from src b limit 10;

explain analyze select key from src;

explain analyze create table t as select key from src;

create table t as select key from src;

explain analyze insert overwrite table t select key from src;

explain analyze select key from src limit 10;

explain analyze select key from src where value < 10;

explain analyze select key from src where key < 10;
select count(*) from (select key from src where key < 10)subq;

explain analyze select key, count(key) from src group by key;
select count(*) from (select key, count(key) from src group by key)subq;

explain analyze select count(*) from src a join src b on a.key = b.value where a.key > 0;

explain analyze select count(*) from src a join src b on a.key = b.key where a.key > 0;
select count(*) from src a join src b on a.key = b.key where a.key > 0;


explain analyze select * from src a union all select * from src b;
select count(*) from (select * from src a union all select * from src b)subq;

set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=10000;

EXPLAIN analyze 
SELECT x.key, y.value
FROM src x JOIN src y ON (x.key = y.key);
