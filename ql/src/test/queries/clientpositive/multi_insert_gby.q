--! qt:dataset:src
-- SORT_QUERY_RESULTS

--HIVE-3699 Multiple insert overwrite into multiple tables query stores same results in all tables
create table e1_n0 (key string, count int);
create table e2_n1 (key string, count int);

explain FROM src
INSERT OVERWRITE TABLE e1_n0
    SELECT key, COUNT(*) WHERE key>450 GROUP BY key
INSERT OVERWRITE TABLE e2_n1
    SELECT key, COUNT(*) WHERE key>500 GROUP BY key;

FROM src
INSERT OVERWRITE TABLE e1_n0
    SELECT key, COUNT(*) WHERE key>450 GROUP BY key
INSERT OVERWRITE TABLE e2_n1
    SELECT key, COUNT(*) WHERE key>500 GROUP BY key;

select * from e1_n0;
select * from e2_n1;

explain FROM src
INSERT OVERWRITE TABLE e1_n0
    SELECT key, COUNT(*) WHERE key>450 GROUP BY key
INSERT OVERWRITE TABLE e2_n1
    SELECT key, COUNT(*) GROUP BY key;

FROM src
INSERT OVERWRITE TABLE e1_n0
    SELECT key, COUNT(*) WHERE key>450 GROUP BY key
INSERT OVERWRITE TABLE e2_n1
    SELECT key, COUNT(*) GROUP BY key;

select * from e1_n0;
select * from e2_n1;
