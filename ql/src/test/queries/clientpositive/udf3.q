--! qt:dataset:src
set hive.mapred.mode=nonstrict;
CREATE TABLE dest1_n104(c1 STRING, c2 STRING, c3 STRING, c4 STRING, c5 STRING) STORED AS TEXTFILE;

EXPLAIN
FROM src INSERT OVERWRITE TABLE dest1_n104 SELECT count(CAST('' AS INT)), sum(CAST('' AS INT)), avg(CAST('' AS INT)), 
min(CAST('' AS INT)), max(CAST('' AS INT));

FROM src INSERT OVERWRITE TABLE dest1_n104 SELECT count(CAST('' AS INT)), sum(CAST('' AS INT)), avg(CAST('' AS INT)), 
min(CAST('' AS INT)), max(CAST('' AS INT));

SELECT dest1_n104.* FROM dest1_n104;
