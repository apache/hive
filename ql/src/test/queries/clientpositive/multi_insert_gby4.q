--! qt:dataset:src
-- SORT_QUERY_RESULTS

create table e1_n4 (key string, count int);
create table e2_n5 (key string, count int);
create table e3_n0 (key string, count int);

explain
FROM (SELECT key, value FROM src) a
INSERT OVERWRITE TABLE e1_n4
    SELECT key, COUNT(*) WHERE key>450 GROUP BY key
INSERT OVERWRITE TABLE e2_n5
    SELECT key, COUNT(*) WHERE key>500 GROUP BY key
INSERT OVERWRITE TABLE e3_n0
    SELECT key, COUNT(*) WHERE key>490 GROUP BY key;

FROM (SELECT key, value FROM src) a
INSERT OVERWRITE TABLE e1_n4
    SELECT key, COUNT(*) WHERE key>450 GROUP BY key
INSERT OVERWRITE TABLE e2_n5
    SELECT key, COUNT(*) WHERE key>500 GROUP BY key
INSERT OVERWRITE TABLE e3_n0
    SELECT key, COUNT(*) WHERE key>490 GROUP BY key;

select * from e1_n4;
select * from e2_n5;
select * from e3_n0;
