set hive.vectorized.execution.enabled=false;
SET hive.optimize.index.filter=true;
SET hive.optimize.ppd=true;

-- Test predicate with partitioned columns
CREATE TEMPORARY TABLE part1_n1_temp (id int, content string) PARTITIONED BY (p string) STORED AS PARQUET;
ALTER TABLE part1_n1_temp ADD PARTITION (p='p1');
INSERT INTO TABLE part1_n1_temp PARTITION (p='p1') VALUES (1, 'a'), (2, 'b');
SELECT * FROM part1_n1_temp;
DROP TABLE part1_n1_temp PURGE;