--! qt:dataset:src
set hive.mapred.mode=nonstrict;
--HIVE-3699 Multiple insert overwrite into multiple tables query stores same results in all tables
create table e1 (count int);
create table e2_n0 (percentile double);
set hive.stats.dbclass=fs;
explain
FROM (select key, cast(key as double) as value from src order by key) a
INSERT OVERWRITE TABLE e1
    SELECT COUNT(*)
INSERT OVERWRITE TABLE e2_n0
    SELECT percentile_approx(value, 0.5);

FROM (select key, cast(key as double) as value from src order by key) a
INSERT OVERWRITE TABLE e1
    SELECT COUNT(*)
INSERT OVERWRITE TABLE e2_n0
    SELECT percentile_approx(value, 0.5);

select * from e1;
select * from e2_n0;
