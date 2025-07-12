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

DROP TABLE IF EXISTS decimal_part1;

CREATE EXTERNAL TABLE decimal_part1 (quantity INT) PARTITIONED BY (sales_percent DECIMAL(10,2)) STORED AS ORC;
INSERT INTO decimal_part1 VALUES (1, 24518.01);
INSERT INTO decimal_part1 VALUES (2, 24518.02);

set hive.auto.convert.join=true;

SELECT count(*), sales_percent FROM decimal_part1 GROUP BY sales_percent;
SELECT d1.quantity,d1.sales_percent FROM decimal_part1 d1 JOIN decimal_part1 d2 ON d1.sales_percent=d2.sales_percent;
SET hive.vectorized.execution.enabled=false;
SELECT d1.quantity,d1.sales_percent FROM decimal_part1 d1 JOIN decimal_part1 d2 ON d1.sales_percent=d2.sales_percent;
