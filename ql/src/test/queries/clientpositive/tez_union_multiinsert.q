--! qt:dataset:src
set hive.explain.user=false;
-- SORT_QUERY_RESULTS

CREATE TABLE DEST1_n150(key STRING, value STRING) STORED AS TEXTFILE;

CREATE TABLE DEST2_n39(key STRING, val1 STRING, val2 STRING) STORED AS TEXTFILE;

explain
FROM (
      select key, value from (
      select 'tst1' as key, cast(count(1) as string) as value, 'tst1' as value2 from src s1
                         UNION all 
      select s2.key as key, s2.value as value, 'tst1' as value2 from src s2) unionsub
                         UNION all
      select key, value from src s0
                             ) unionsrc
INSERT OVERWRITE TABLE DEST1_n150 SELECT unionsrc.key, COUNT(DISTINCT SUBSTR(unionsrc.value,5)) GROUP BY unionsrc.key
INSERT OVERWRITE TABLE DEST2_n39 SELECT unionsrc.key, unionsrc.value, COUNT(DISTINCT SUBSTR(unionsrc.value,5)) 
GROUP BY unionsrc.key, unionsrc.value;

FROM (
      select key, value from (
      select 'tst1' as key, cast(count(1) as string) as value, 'tst1' as value2 from src s1
                         UNION all 
      select s2.key as key, s2.value as value, 'tst1' as value2 from src s2) unionsub
                         UNION all
      select key, value from src s0
                             ) unionsrc
INSERT OVERWRITE TABLE DEST1_n150 SELECT unionsrc.key, COUNT(DISTINCT SUBSTR(unionsrc.value,5)) GROUP BY unionsrc.key
INSERT OVERWRITE TABLE DEST2_n39 SELECT unionsrc.key, unionsrc.value, COUNT(DISTINCT SUBSTR(unionsrc.value,5)) 
GROUP BY unionsrc.key, unionsrc.value;

select * from DEST1_n150;
select * from DEST2_n39;

explain
FROM (
      select key, value from src s0
                         UNION all
      select key, value from (
      select 'tst1' as key, cast(count(1) as string) as value, 'tst1' as value2 from src s1
                         UNION all 
      select s2.key as key, s2.value as value, 'tst1' as value2 from src s2) unionsub) unionsrc
INSERT OVERWRITE TABLE DEST1_n150 SELECT unionsrc.key, COUNT(DISTINCT SUBSTR(unionsrc.value,5)) GROUP BY unionsrc.key
INSERT OVERWRITE TABLE DEST2_n39 SELECT unionsrc.key, unionsrc.value, COUNT(DISTINCT SUBSTR(unionsrc.value,5)) 
GROUP BY unionsrc.key, unionsrc.value;

FROM (
      select key, value from src s0
                         UNION all
      select key, value from (
      select 'tst1' as key, cast(count(1) as string) as value, 'tst1' as value2 from src s1
                         UNION all 
      select s2.key as key, s2.value as value, 'tst1' as value2 from src s2) unionsub) unionsrc
INSERT OVERWRITE TABLE DEST1_n150 SELECT unionsrc.key, COUNT(DISTINCT SUBSTR(unionsrc.value,5)) GROUP BY unionsrc.key
INSERT OVERWRITE TABLE DEST2_n39 SELECT unionsrc.key, unionsrc.value, COUNT(DISTINCT SUBSTR(unionsrc.value,5)) 
GROUP BY unionsrc.key, unionsrc.value;

select * from DEST1_n150;
select * from DEST2_n39;


explain
FROM (
      select key, value from src s0
                         UNION all
      select 'tst1' as key, cast(count(1) as string) as value from src s1
                         UNION all 
      select s2.key as key, s2.value as value from src s2) unionsrc
INSERT OVERWRITE TABLE DEST1_n150 SELECT unionsrc.key, COUNT(DISTINCT SUBSTR(unionsrc.value,5)) GROUP BY unionsrc.key
INSERT OVERWRITE TABLE DEST2_n39 SELECT unionsrc.key, unionsrc.value, COUNT(DISTINCT SUBSTR(unionsrc.value,5)) 
GROUP BY unionsrc.key, unionsrc.value;

FROM (
      select key, value from src s0
                         UNION all
      select 'tst1' as key, cast(count(1) as string) as value from src s1
                         UNION all 
      select s2.key as key, s2.value as value from src s2) unionsrc
INSERT OVERWRITE TABLE DEST1_n150 SELECT unionsrc.key, COUNT(DISTINCT SUBSTR(unionsrc.value,5)) GROUP BY unionsrc.key
INSERT OVERWRITE TABLE DEST2_n39 SELECT unionsrc.key, unionsrc.value, COUNT(DISTINCT SUBSTR(unionsrc.value,5)) 
GROUP BY unionsrc.key, unionsrc.value;

select * from DEST1_n150;
select * from DEST2_n39;

explain 
FROM (select 'tst1' as key, cast(count(1) as string) as value from src s1
                         UNION all 
      select s2.key as key, s2.value as value from src s2) unionsrc
INSERT OVERWRITE TABLE DEST1_n150 SELECT unionsrc.key, COUNT(DISTINCT SUBSTR(unionsrc.value,5)) GROUP BY unionsrc.key
INSERT OVERWRITE TABLE DEST2_n39 SELECT unionsrc.key, unionsrc.value, COUNT(DISTINCT SUBSTR(unionsrc.value,5)) 
GROUP BY unionsrc.key, unionsrc.value;

FROM (select 'tst1' as key, cast(count(1) as string) as value from src s1
                         UNION all 
      select s2.key as key, s2.value as value from src s2) unionsrc
INSERT OVERWRITE TABLE DEST1_n150 SELECT unionsrc.key, COUNT(DISTINCT SUBSTR(unionsrc.value,5)) GROUP BY unionsrc.key
INSERT OVERWRITE TABLE DEST2_n39 SELECT unionsrc.key, unionsrc.value, COUNT(DISTINCT SUBSTR(unionsrc.value,5)) 
GROUP BY unionsrc.key, unionsrc.value;

select * from DEST1_n150;
select * from DEST2_n39;

explain
FROM (select 'tst1' as key, cast(count(1) as string) as value from src s1
                         UNION distinct 
      select s2.key as key, s2.value as value from src s2) unionsrc
INSERT OVERWRITE TABLE DEST1_n150 SELECT unionsrc.key, COUNT(DISTINCT SUBSTR(unionsrc.value,5)) GROUP BY unionsrc.key
INSERT OVERWRITE TABLE DEST2_n39 SELECT unionsrc.key, unionsrc.value, COUNT(DISTINCT SUBSTR(unionsrc.value,5)) 
GROUP BY unionsrc.key, unionsrc.value;

FROM (select 'tst1' as key, cast(count(1) as string) as value from src s1
                         UNION distinct 
      select s2.key as key, s2.value as value from src s2) unionsrc
INSERT OVERWRITE TABLE DEST1_n150 SELECT unionsrc.key, COUNT(DISTINCT SUBSTR(unionsrc.value,5)) GROUP BY unionsrc.key
INSERT OVERWRITE TABLE DEST2_n39 SELECT unionsrc.key, unionsrc.value, COUNT(DISTINCT SUBSTR(unionsrc.value,5)) 
GROUP BY unionsrc.key, unionsrc.value;

select * from DEST1_n150;
select * from DEST2_n39;