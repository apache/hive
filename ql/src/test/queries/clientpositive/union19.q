--! qt:dataset:src
-- SORT_QUERY_RESULTS

CREATE TABLE DEST1_n86(key STRING, value STRING) STORED AS TEXTFILE;
CREATE TABLE DEST2_n21(key STRING, val1 STRING, val2 STRING) STORED AS TEXTFILE;

-- union case:map-reduce sub-queries followed by multi-table insert

explain 
FROM (select 'tst1' as key, cast(count(1) as string) as value from src s1
                         UNION  ALL  
      select s2.key as key, s2.value as value from src s2) unionsrc
INSERT OVERWRITE TABLE DEST1_n86 SELECT unionsrc.key, count(unionsrc.value) group by unionsrc.key
INSERT OVERWRITE TABLE DEST2_n21 SELECT unionsrc.key, unionsrc.value, unionsrc.value;

FROM (select 'tst1' as key, cast(count(1) as string) as value from src s1
                         UNION  ALL  
      select s2.key as key, s2.value as value from src s2) unionsrc
INSERT OVERWRITE TABLE DEST1_n86 SELECT unionsrc.key, count(unionsrc.value) group by unionsrc.key
INSERT OVERWRITE TABLE DEST2_n21 SELECT unionsrc.key, unionsrc.value, unionsrc.value;

SELECT DEST1_n86.* FROM DEST1_n86 SORT BY DEST1_n86.key, DEST1_n86.value;
SELECT DEST2_n21.* FROM DEST2_n21 SORT BY DEST2_n21.key, DEST2_n21.val1, DEST2_n21.val2;



