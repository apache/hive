--! qt:dataset:src
set hive.mapred.mode=nonstrict;
-- SORT_QUERY_RESULTS
create table e1_n2 (key string, keyD double);
create table e2_n3 (key string, keyD double, value string);
create table e3 (key string, keyD double);
set hive.stats.dbclass=fs;
explain
FROM (select key, cast(key as double) as keyD, value from src order by key) a
INSERT OVERWRITE TABLE e1_n2
    SELECT key, COUNT(distinct value) group by key
INSERT OVERWRITE TABLE e2_n3
    SELECT key, sum(keyD), value group by key, value;

explain
FROM (select key, cast(key as double) as keyD, value from src order by key) a
INSERT OVERWRITE TABLE e2_n3
    SELECT key, sum(keyD), value group by key, value
INSERT OVERWRITE TABLE e1_n2
    SELECT key, COUNT(distinct value) group by key;

FROM (select key, cast(key as double) as keyD, value from src order by key) a
INSERT OVERWRITE TABLE e1_n2
    SELECT key, COUNT(distinct value) group by key
INSERT OVERWRITE TABLE e2_n3
    SELECT key, sum(keyD), value group by key, value;

select * from e1_n2;
select * from e2_n3;

FROM (select key, cast(key as double) as keyD, value from src order by key) a
INSERT OVERWRITE TABLE e2_n3
    SELECT key, sum(keyD), value group by key, value
INSERT OVERWRITE TABLE e1_n2
    SELECT key, COUNT(distinct value) group by key;

select * from e1_n2;
select * from e2_n3;

explain
from src
insert overwrite table e1_n2
select key, count(distinct value) group by key
insert overwrite table e3
select value, count(distinct key) group by value;


explain
FROM (select key, cast(key as double) as keyD, value from src order by key) a
INSERT OVERWRITE TABLE e1_n2
    SELECT key, COUNT(distinct value) group by key
INSERT OVERWRITE TABLE e2_n3
    SELECT key, sum(keyD), value group by key, value
INSERT overwrite table e3
    SELECT key, COUNT(distinct keyD) group by key, keyD, value;
