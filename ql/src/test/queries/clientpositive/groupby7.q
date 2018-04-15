--! qt:dataset:src
set hive.map.aggr=false;
set hive.groupby.skewindata=true;

-- SORT_QUERY_RESULTS

CREATE TABLE DEST1(key INT, value STRING) STORED AS TEXTFILE;
CREATE TABLE DEST2(key INT, value STRING) STORED AS TEXTFILE;

SET hive.exec.compress.intermediate=true;
SET hive.exec.compress.output=true; 

FROM SRC
INSERT OVERWRITE TABLE DEST1 SELECT SRC.key, sum(SUBSTR(SRC.value,5)) GROUP BY SRC.key
INSERT OVERWRITE TABLE DEST2 SELECT SRC.key, sum(SUBSTR(SRC.value,5)) GROUP BY SRC.key;

SELECT DEST1.* FROM DEST1;
SELECT DEST2.* FROM DEST2;
