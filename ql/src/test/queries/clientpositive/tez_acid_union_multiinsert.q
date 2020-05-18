--! qt:dataset:src
set hive.explain.user=false;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.support.concurrency=true;
set hive.acid.direct.insert.enabled=true;

-- SORT_QUERY_RESULTS

DROP TABLE IF EXISTS DEST1_acid_1;
DROP TABLE IF EXISTS DEST1_acid_2;
DROP TABLE IF EXISTS DEST1_acid_3;
DROP TABLE IF EXISTS DEST1_acid_4;
DROP TABLE IF EXISTS DEST1_acid_5;
DROP TABLE IF EXISTS DEST1_acid_6;

CREATE TABLE DEST1_acid_1(key STRING, value STRING) STORED AS ORC TBLPROPERTIES('transactional'='true');
CREATE TABLE DEST1_acid_2(key STRING, val1 STRING, val2 STRING) STORED AS ORC TBLPROPERTIES('transactional'='true');
CREATE TABLE DEST1_acid_3(key STRING, value STRING) STORED AS ORC TBLPROPERTIES('transactional'='true');
CREATE TABLE DEST1_acid_4(key STRING, val1 STRING, val2 STRING) STORED AS ORC TBLPROPERTIES('transactional'='true');
CREATE TABLE DEST1_acid_5(key STRING, value STRING) STORED AS ORC TBLPROPERTIES('transactional'='true');
CREATE TABLE DEST1_acid_6(key STRING, val1 STRING, val2 STRING) STORED AS ORC TBLPROPERTIES('transactional'='true');
CREATE TABLE DEST1_acid_7(key STRING, value STRING) STORED AS ORC TBLPROPERTIES('transactional'='true');
CREATE TABLE DEST1_acid_8(key STRING, val1 STRING, val2 STRING) STORED AS ORC TBLPROPERTIES('transactional'='true');
CREATE TABLE DEST1_acid_9(key STRING, value STRING) STORED AS ORC TBLPROPERTIES('transactional'='true');
CREATE TABLE DEST1_acid_10(key STRING, val1 STRING, val2 STRING) STORED AS ORC TBLPROPERTIES('transactional'='true');

FROM (
      select key, value from (
      select 'tst1' as key, cast(count(1) as string) as value, 'tst1' as value2 from src s1
                         UNION all 
      select s2.key as key, s2.value as value, 'tst1' as value2 from src s2) unionsub
                         UNION all
      select key, value from src s0
                             ) unionsrc
INSERT INTO TABLE DEST1_acid_1 SELECT unionsrc.key, COUNT(DISTINCT SUBSTR(unionsrc.value,5)) GROUP BY unionsrc.key
INSERT INTO TABLE DEST1_acid_2 SELECT unionsrc.key, unionsrc.value, COUNT(DISTINCT SUBSTR(unionsrc.value,5)) 
GROUP BY unionsrc.key, unionsrc.value;

select * from DEST1_acid_1;
select * from DEST1_acid_2;

FROM (
      select key, value from src s0
                         UNION all
      select key, value from (
      select 'tst1' as key, cast(count(1) as string) as value, 'tst1' as value2 from src s1
                         UNION all 
      select s2.key as key, s2.value as value, 'tst1' as value2 from src s2) unionsub) unionsrc
INSERT INTO TABLE DEST1_acid_3 SELECT unionsrc.key, COUNT(DISTINCT SUBSTR(unionsrc.value,5)) GROUP BY unionsrc.key
INSERT INTO TABLE DEST1_acid_4 SELECT unionsrc.key, unionsrc.value, COUNT(DISTINCT SUBSTR(unionsrc.value,5)) 
GROUP BY unionsrc.key, unionsrc.value;

select * from DEST1_acid_3;
select * from DEST1_acid_4;

FROM (
      select key, value from src s0
                         UNION all
      select 'tst1' as key, cast(count(1) as string) as value from src s1
                         UNION all 
      select s2.key as key, s2.value as value from src s2) unionsrc
INSERT INTO TABLE DEST1_acid_5 SELECT unionsrc.key, COUNT(DISTINCT SUBSTR(unionsrc.value,5)) GROUP BY unionsrc.key
INSERT INTO TABLE DEST1_acid_6 SELECT unionsrc.key, unionsrc.value, COUNT(DISTINCT SUBSTR(unionsrc.value,5)) 
GROUP BY unionsrc.key, unionsrc.value;

select * from DEST1_acid_5;
select * from DEST1_acid_6;

FROM (select 'tst1' as key, cast(count(1) as string) as value from src s1
                         UNION all 
      select s2.key as key, s2.value as value from src s2) unionsrc
INSERT INTO TABLE DEST1_acid_7 SELECT unionsrc.key, COUNT(DISTINCT SUBSTR(unionsrc.value,5)) GROUP BY unionsrc.key
INSERT INTO TABLE DEST1_acid_8 SELECT unionsrc.key, unionsrc.value, COUNT(DISTINCT SUBSTR(unionsrc.value,5)) 
GROUP BY unionsrc.key, unionsrc.value;

select * from DEST1_acid_7;
select * from DEST1_acid_8;

FROM (select 'tst1' as key, cast(count(1) as string) as value from src s1
                         UNION distinct 
      select s2.key as key, s2.value as value from src s2) unionsrc
INSERT INTO TABLE DEST1_acid_9 SELECT unionsrc.key, COUNT(DISTINCT SUBSTR(unionsrc.value,5)) GROUP BY unionsrc.key
INSERT INTO TABLE DEST1_acid_10 SELECT unionsrc.key, unionsrc.value, COUNT(DISTINCT SUBSTR(unionsrc.value,5)) 
GROUP BY unionsrc.key, unionsrc.value;

select * from DEST1_acid_9;
select * from DEST1_acid_10;

DROP TABLE IF EXISTS DEST1_acid_1;
DROP TABLE IF EXISTS DEST1_acid_2;
DROP TABLE IF EXISTS DEST1_acid_3;
DROP TABLE IF EXISTS DEST1_acid_4;
DROP TABLE IF EXISTS DEST1_acid_5;
DROP TABLE IF EXISTS DEST1_acid_6;