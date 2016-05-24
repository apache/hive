set hive.stats.column.autogather=true;
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
set hive.map.aggr=false;
set hive.groupby.skewindata=true;

-- Taken from groupby2.q
CREATE TABLE dest_g2(key STRING, c1 INT, c2 STRING) STORED AS TEXTFILE;
CREATE TEMPORARY TABLE src_temp AS SELECT * FROM src;

explain FROM src_temp
INSERT OVERWRITE TABLE dest_g2 SELECT substr(src_temp.key,1,1), count(DISTINCT substr(src_temp.value,5)), concat(substr(src_temp.key,1,1),sum(substr(src_temp.value,5))) GROUP BY substr(src_temp.key,1,1);

FROM src_temp
INSERT OVERWRITE TABLE dest_g2 SELECT substr(src_temp.key,1,1), count(DISTINCT substr(src_temp.value,5)), concat(substr(src_temp.key,1,1),sum(substr(src_temp.value,5))) GROUP BY substr(src_temp.key,1,1);

SELECT dest_g2.* FROM dest_g2;

DROP TABLE dest_g2;
DROP TABLE src_temp;
