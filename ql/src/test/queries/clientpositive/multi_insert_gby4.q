-- SORT_QUERY_RESULTS

create table e1 (key string, count int);
create table e2 (key string, count int);
create table e3 (key string, count int);

explain
FROM (SELECT key, value FROM src) a
INSERT OVERWRITE TABLE e1
    SELECT key, COUNT(*) WHERE key>450 GROUP BY key
INSERT OVERWRITE TABLE e2
    SELECT key, COUNT(*) WHERE key>500 GROUP BY key
INSERT OVERWRITE TABLE e3
    SELECT key, COUNT(*) WHERE key>490 GROUP BY key;

FROM (SELECT key, value FROM src) a
INSERT OVERWRITE TABLE e1
    SELECT key, COUNT(*) WHERE key>450 GROUP BY key
INSERT OVERWRITE TABLE e2
    SELECT key, COUNT(*) WHERE key>500 GROUP BY key
INSERT OVERWRITE TABLE e3
    SELECT key, COUNT(*) WHERE key>490 GROUP BY key;

select * from e1;
select * from e2;
select * from e3;
