set hive.compute.query.using.stats=true;
DROP TABLE insert_into2;
CREATE TABLE insert_into2 (key int, value string) 
  PARTITIONED BY (ds string);

EXPLAIN INSERT INTO TABLE insert_into2 PARTITION (ds='1') 
  SELECT * FROM src LIMIT 100;
INSERT INTO TABLE insert_into2 PARTITION (ds='1') SELECT * FROM src limit 100;
explain
select count (*) from insert_into2 where ds = '1';
select count (*) from insert_into2 where ds = '1';
INSERT INTO TABLE insert_into2 PARTITION (ds='1') SELECT * FROM src limit 100;
explain
SELECT COUNT(*) FROM insert_into2 WHERE ds='1';
SELECT COUNT(*) FROM insert_into2 WHERE ds='1';
SELECT SUM(HASH(c)) FROM (
    SELECT TRANSFORM(*) USING 'tr \t _' AS (c) FROM insert_into2
) t;

EXPLAIN INSERT OVERWRITE TABLE insert_into2 PARTITION (ds='2')
  SELECT * FROM src LIMIT 100;
INSERT OVERWRITE TABLE insert_into2 PARTITION (ds='2')
  SELECT * FROM src LIMIT 100;

SELECT SUM(HASH(c)) FROM (
    SELECT TRANSFORM(*) USING 'tr \t _' AS (c) FROM insert_into2
) t;
explain
SELECT COUNT(*) FROM insert_into2 WHERE ds='2';
SELECT COUNT(*) FROM insert_into2 WHERE ds='2';

EXPLAIN INSERT OVERWRITE TABLE insert_into2 PARTITION (ds='2')
  SELECT * FROM src LIMIT 50;
INSERT OVERWRITE TABLE insert_into2 PARTITION (ds='2')
  SELECT * FROM src LIMIT 50;
SELECT SUM(HASH(c)) FROM (
    SELECT TRANSFORM(*) USING 'tr \t _' AS (c) FROM insert_into2
) t;
explain
SELECT COUNT(*) FROM insert_into2 WHERE ds='2';
SELECT COUNT(*) FROM insert_into2 WHERE ds='2';


DROP TABLE insert_into2;

set hive.compute.query.using.stats=false;
