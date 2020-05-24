set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;

DROP TABLE IF EXISTS decimal_part;

CREATE TABLE decimal_part (id DECIMAL(4,0), foo VARCHAR(10))
    PARTITIONED BY (nr_bank DECIMAL(4,0))
STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB');

INSERT INTO decimal_part PARTITION (nr_bank = 88) VALUES (1, 'test');
INSERT INTO decimal_part PARTITION (nr_bank = 8801) VALUES (1, '8801');

EXPLAIN VECTORIZATION EXPRESSION SELECT count(*), nr_bank FROM decimal_part GROUP BY nr_bank;
SELECT count(*), nr_bank FROM decimal_part GROUP BY nr_bank;