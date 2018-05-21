--! qt:dataset:src
CREATE TABLE DEST1_n128(key STRING, value STRING) STORED AS TEXTFILE;
CREATE TABLE DEST2_n33(key STRING, val1 STRING, val2 STRING) STORED AS TEXTFILE;

-- SORT_QUERY_RESULTS

-- union case:map-reduce sub-queries followed by multi-table insert 

explain 
FROM (select 'tst1' as key, cast(count(1) as string) as value from src s1
                         UNION  ALL  
      select s2.key as key, s2.value as value from src s2) unionsrc
INSERT OVERWRITE TABLE DEST1_n128 SELECT unionsrc.key, unionsrc.value
INSERT OVERWRITE TABLE DEST2_n33 SELECT unionsrc.key, unionsrc.value, unionsrc.value;

FROM (select 'tst1' as key, cast(count(1) as string) as value from src s1
                         UNION  ALL  
      select s2.key as key, s2.value as value from src s2) unionsrc
INSERT OVERWRITE TABLE DEST1_n128 SELECT unionsrc.key, unionsrc.value
INSERT OVERWRITE TABLE DEST2_n33 SELECT unionsrc.key, unionsrc.value, unionsrc.value;

SELECT DEST1_n128.* FROM DEST1_n128 SORT BY DEST1_n128.key, DEST1_n128.value;
SELECT DEST2_n33.* FROM DEST2_n33 SORT BY DEST2_n33.key, DEST2_n33.val1, DEST2_n33.val2;
