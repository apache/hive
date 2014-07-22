DROP TABLE IF EXISTS dec_comp;

CREATE TABLE dec_comp(arr ARRAY<decimal(5,2)>, m MAP<String, decimal(5,2)>, s STRUCT<i:int, d:decimal(5,2)>)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' COLLECTION ITEMS TERMINATED BY ','  MAP KEYS TERMINATED by ':';

LOAD DATA LOCAL INPATH '../../data/files/dec_comp.txt' INTO TABLE dec_comp;

SELECT * FROM dec_comp;

DROP TABLE IF EXISTS parq_dec_comp;

CREATE TABLE parq_dec_comp(arr ARRAY<decimal(5,2)>, m MAP<String, decimal(5,2)>, s STRUCT<i:int, d:decimal(5,2)>)
STORED AS PARQUET;

DESC parq_dec_comp;

INSERT OVERWRITE TABLE parq_dec_comp SELECT * FROM dec_comp;

SELECT * FROM parq_dec_comp;

DROP TABLE dec_comp;
DROP TABLE parq_dec_comp;
