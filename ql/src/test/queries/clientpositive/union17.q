--! qt:dataset:src
CREATE TABLE DEST1_n78(key STRING, value STRING) STORED AS TEXTFILE;
CREATE TABLE DEST2_n17(key STRING, val1 STRING, val2 STRING) STORED AS TEXTFILE;

-- SORT_QUERY_RESULTS
-- union case:map-reduce sub-queries followed by multi-table insert

explain 
FROM (select 'tst1' as key, cast(count(1) as string) as value from src s1
                         UNION  ALL  
      select s2.key as key, s2.value as value from src s2) unionsrc
INSERT OVERWRITE TABLE DEST1_n78 SELECT unionsrc.key, COUNT(DISTINCT SUBSTR(unionsrc.value,5)) GROUP BY unionsrc.key
INSERT OVERWRITE TABLE DEST2_n17 SELECT unionsrc.key, unionsrc.value, COUNT(DISTINCT SUBSTR(unionsrc.value,5)) GROUP BY unionsrc.key, unionsrc.value;

FROM (select 'tst1' as key, cast(count(1) as string) as value from src s1
                         UNION  ALL  
      select s2.key as key, s2.value as value from src s2) unionsrc
INSERT OVERWRITE TABLE DEST1_n78 SELECT unionsrc.key, COUNT(DISTINCT SUBSTR(unionsrc.value,5)) GROUP BY unionsrc.key
INSERT OVERWRITE TABLE DEST2_n17 SELECT unionsrc.key, unionsrc.value, COUNT(DISTINCT SUBSTR(unionsrc.value,5)) GROUP BY unionsrc.key, unionsrc.value;

SELECT DEST1_n78.* FROM DEST1_n78;
SELECT DEST2_n17.* FROM DEST2_n17;
