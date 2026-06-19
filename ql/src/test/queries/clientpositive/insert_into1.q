--! qt:dataset:src
set hive.explain.user=false;
set hive.compute.query.using.stats=true;

-- SORT_QUERY_RESULTS

DROP TABLE insert_into1;

CREATE TABLE insert_into1 (key int, value string);

EXPLAIN INSERT INTO TABLE insert_into1 SELECT * from src ORDER BY key LIMIT 100;
INSERT INTO TABLE insert_into1 SELECT * from src ORDER BY key LIMIT 100;
SELECT SUM(HASH(c)) FROM (
    SELECT TRANSFORM(*) USING 'tr \t _' AS (c) FROM insert_into1
) t;
explain 
select count(*) from insert_into1;
select count(*) from insert_into1;
EXPLAIN INSERT INTO TABLE insert_into1 SELECT * FROM src ORDER BY key LIMIT 100;
INSERT INTO TABLE insert_into1 SELECT * FROM src ORDER BY key LIMIT 100;
SELECT SUM(HASH(c)) FROM (
    SELECT TRANSFORM(*) USING 'tr \t _' AS (c) FROM insert_into1
) t;

explain
SELECT COUNT(*) FROM insert_into1;
select count(*) from insert_into1;

EXPLAIN INSERT OVERWRITE TABLE insert_into1 SELECT * FROM src ORDER BY key LIMIT 10;
INSERT OVERWRITE TABLE insert_into1 SELECT * FROM src ORDER BY key LIMIT 10;
SELECT SUM(HASH(c)) FROM (
    SELECT TRANSFORM(*) USING 'tr \t _' AS (c) FROM insert_into1
) t;

explain
SELECT COUNT(*) FROM insert_into1;
select count(*) from insert_into1;

explain insert overwrite table insert_into1 select 1, 'a';
insert overwrite table insert_into1 select 1, 'a';

explain insert into insert_into1 select 2, 'b';
insert into insert_into1 select 2, 'b';

select * from insert_into1;

set hive.stats.autogather=false;                                                                                                                                    
explain
insert into table insert_into1 values(1, 'abc');                                                                                                                    
insert into table insert_into1 values(1, 'abc');                                                                                                                    
explain
SELECT COUNT(*) FROM insert_into1;                                                                                                                                  
select count(*) from insert_into1;

DROP TABLE insert_into1;
set hive.stats.autogather=true;
set hive.compute.query.using.stats=false;
