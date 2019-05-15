--! qt:dataset:srcpart
CREATE TABLE dest1_n84(key INT, value STRING, hr STRING, ds STRING) STORED AS TEXTFILE;
CREATE TABLE dest2_n20(key INT, value STRING, hr STRING, ds STRING) STORED AS TEXTFILE;

-- SORT_QUERY_RESULTS

EXPLAIN EXTENDED
FROM srcpart
INSERT OVERWRITE TABLE dest1_n84 SELECT srcpart.key, srcpart.value, srcpart.hr, srcpart.ds WHERE srcpart.key < 100 and srcpart.ds = '2008-04-08' and srcpart.hr = '12'
INSERT OVERWRITE TABLE dest2_n20 SELECT srcpart.key, srcpart.value, srcpart.hr, srcpart.ds WHERE srcpart.key < 100 and srcpart.ds = '2008-04-09' and srcpart.hr = '12';

FROM srcpart
INSERT OVERWRITE TABLE dest1_n84 SELECT srcpart.key, srcpart.value, srcpart.hr, srcpart.ds WHERE srcpart.key < 100 and srcpart.ds = '2008-04-08' and srcpart.hr = '12'
INSERT OVERWRITE TABLE dest2_n20 SELECT srcpart.key, srcpart.value, srcpart.hr, srcpart.ds WHERE srcpart.key < 100 and srcpart.ds = '2008-04-09' and srcpart.hr = '12';

SELECT dest1_n84.* FROM dest1_n84 sort by key,value,ds,hr;
SELECT dest2_n20.* FROM dest2_n20 sort by key,value,ds,hr;


