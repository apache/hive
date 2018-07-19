--! qt:dataset:src
set hive.map.aggr=false;
set hive.groupby.skewindata=true;

-- SORT_QUERY_RESULTS

CREATE TABLE DEST1_n132(key INT, value STRING) STORED AS TEXTFILE;
CREATE TABLE DEST2_n34(key INT, value STRING) STORED AS TEXTFILE;

SET hive.exec.compress.intermediate=true;
SET hive.exec.compress.output=true; 

FROM SRC
INSERT OVERWRITE TABLE DEST1_n132 SELECT SRC.key, sum(SUBSTR(SRC.value,5)) GROUP BY SRC.key
INSERT OVERWRITE TABLE DEST2_n34 SELECT SRC.key, sum(SUBSTR(SRC.value,5)) GROUP BY SRC.key;

SELECT DEST1_n132.* FROM DEST1_n132;
SELECT DEST2_n34.* FROM DEST2_n34;
