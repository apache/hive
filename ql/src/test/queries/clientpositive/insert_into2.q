--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.compute.query.using.stats=true;
DROP TABLE insert_into2;
CREATE TABLE insert_into2 (key int, value string) 
  PARTITIONED BY (ds string);

EXPLAIN INSERT INTO TABLE insert_into2 PARTITION (ds='1') 
  SELECT * FROM src order by key LIMIT 100;
INSERT INTO TABLE insert_into2 PARTITION (ds='1') SELECT * FROM src order by key limit 100;
explain
select count (*) from insert_into2 where ds = '1';
select count (*) from insert_into2 where ds = '1';
INSERT INTO TABLE insert_into2 PARTITION (ds='1') SELECT * FROM src order by key limit 100;
explain
SELECT COUNT(*) FROM insert_into2 WHERE ds='1';
SELECT COUNT(*) FROM insert_into2 WHERE ds='1';
SELECT SUM(HASH(c)) FROM (
    SELECT TRANSFORM(*) USING 'tr \t _' AS (c) FROM insert_into2
) t;

EXPLAIN INSERT OVERWRITE TABLE insert_into2 PARTITION (ds='2')
  SELECT * FROM src order by key LIMIT 100;
INSERT OVERWRITE TABLE insert_into2 PARTITION (ds='2')
  SELECT * FROM src order by key LIMIT 100;

SELECT SUM(HASH(c)) FROM (
    SELECT TRANSFORM(*) USING 'tr \t _' AS (c) FROM insert_into2
) t;
explain
SELECT COUNT(*) FROM insert_into2 WHERE ds='2';
SELECT COUNT(*) FROM insert_into2 WHERE ds='2';

EXPLAIN INSERT OVERWRITE TABLE insert_into2 PARTITION (ds='2')
  SELECT * FROM src order by key LIMIT 50;
INSERT OVERWRITE TABLE insert_into2 PARTITION (ds='2')
  SELECT * FROM src order by key LIMIT 50;
SELECT SUM(HASH(c)) FROM (
    SELECT TRANSFORM(*) USING 'tr \t _' AS (c) FROM insert_into2
) t;
explain
SELECT COUNT(*) FROM insert_into2 WHERE ds='2';
SELECT COUNT(*) FROM insert_into2 WHERE ds='2';

set hive.stats.autogather=false;                                                                     

insert into table insert_into2 partition (ds='2') values(1, 'abc');                                                                                                                    
explain
SELECT COUNT(*) FROM insert_into2 where ds='2';                                                                                                                                  
select count(*) from insert_into2 where ds='2';


DROP TABLE insert_into2;

set hive.stats.autogather=true;                                                                                                                                    
set hive.compute.query.using.stats=false;
