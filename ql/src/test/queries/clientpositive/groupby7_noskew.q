--! qt:dataset:src
set hive.map.aggr=false;
set hive.multigroupby.singlereducer=false;
set hive.groupby.skewindata=false;
set mapred.reduce.tasks=31;

-- SORT_QUERY_RESULTS

CREATE TABLE DEST1_n101(key INT, value STRING) STORED AS TEXTFILE;
CREATE TABLE DEST2_n28(key INT, value STRING) STORED AS TEXTFILE;

SET hive.exec.compress.intermediate=true;
SET hive.exec.compress.output=true; 

EXPLAIN
FROM SRC
INSERT OVERWRITE TABLE DEST1_n101 SELECT SRC.key, sum(SUBSTR(SRC.value,5)) GROUP BY SRC.key
INSERT OVERWRITE TABLE DEST2_n28 SELECT SRC.key, sum(SUBSTR(SRC.value,5)) GROUP BY SRC.key;

FROM SRC
INSERT OVERWRITE TABLE DEST1_n101 SELECT SRC.key, sum(SUBSTR(SRC.value,5)) GROUP BY SRC.key
INSERT OVERWRITE TABLE DEST2_n28 SELECT SRC.key, sum(SUBSTR(SRC.value,5)) GROUP BY SRC.key;

SELECT DEST1_n101.* FROM DEST1_n101;
SELECT DEST2_n28.* FROM DEST2_n28;
