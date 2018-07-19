--! qt:dataset:src
set hive.map.aggr=false;
set hive.groupby.skewindata=false;
set mapred.reduce.tasks=31;

-- SORT_QUERY_RESULTS

CREATE TABLE DEST1_n170(key INT, value STRING) STORED AS TEXTFILE;
CREATE TABLE DEST2_n42(key INT, value STRING) STORED AS TEXTFILE;

SET hive.exec.compress.intermediate=true;
SET hive.exec.compress.output=true; 

EXPLAIN
FROM SRC
INSERT OVERWRITE TABLE DEST1_n170 SELECT SRC.key, sum(SUBSTR(SRC.value,5)) GROUP BY SRC.key ORDER BY SRC.key limit 10
INSERT OVERWRITE TABLE DEST2_n42 SELECT SRC.key, sum(SUBSTR(SRC.value,5)) GROUP BY SRC.key ORDER BY SRC.key limit 10;

FROM SRC
INSERT OVERWRITE TABLE DEST1_n170 SELECT SRC.key, sum(SUBSTR(SRC.value,5)) GROUP BY SRC.key ORDER BY SRC.key limit 10
INSERT OVERWRITE TABLE DEST2_n42 SELECT SRC.key, sum(SUBSTR(SRC.value,5)) GROUP BY SRC.key ORDER BY SRC.key limit 10;

SELECT DEST1_n170.* FROM DEST1_n170;
SELECT DEST2_n42.* FROM DEST2_n42;
