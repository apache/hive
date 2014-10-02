set hive.compute.query.using.stats=true;
DROP TABLE insert_into1;

CREATE TABLE insert_into1 (key int, value string);

EXPLAIN INSERT INTO TABLE insert_into1 SELECT * from src LIMIT 100;
INSERT INTO TABLE insert_into1 SELECT * from src LIMIT 100;
SELECT SUM(HASH(c)) FROM (
    SELECT TRANSFORM(*) USING 'tr \t _' AS (c) FROM insert_into1
) t;
explain 
select count(*) from insert_into1;
select count(*) from insert_into1;
EXPLAIN INSERT INTO TABLE insert_into1 SELECT * FROM src LIMIT 100;
INSERT INTO TABLE insert_into1 SELECT * FROM src LIMIT 100;
SELECT SUM(HASH(c)) FROM (
    SELECT TRANSFORM(*) USING 'tr \t _' AS (c) FROM insert_into1
) t;

explain
SELECT COUNT(*) FROM insert_into1;
select count(*) from insert_into1;

EXPLAIN INSERT OVERWRITE TABLE insert_into1 SELECT * FROM src LIMIT 10;
INSERT OVERWRITE TABLE insert_into1 SELECT * FROM src LIMIT 10;
SELECT SUM(HASH(c)) FROM (
    SELECT TRANSFORM(*) USING 'tr \t _' AS (c) FROM insert_into1
) t;

explain
SELECT COUNT(*) FROM insert_into1;
select count(*) from insert_into1;

DROP TABLE insert_into1;

set hive.compute.query.using.stats=false;
