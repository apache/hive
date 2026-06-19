--! qt:dataset:src

EXPLAIN
CREATE TEMPORARY TABLE partition_ctas_1_temp PARTITIONED BY (key) AS
  SELECT value, key FROM src where key > 200 and key < 300;

CREATE TABLE partition_ctas_1_temp PARTITIONED BY (key) AS
  SELECT value, key FROM src where key > 200 and key < 300;

DESCRIBE FORMATTED partition_ctas_1_temp;

EXPLAIN
SELECT * FROM partition_ctas_1_temp where key = 238;

SELECT * FROM partition_ctas_1_temp where key = 238;

CREATE TEMPORARY TABLE partition_ctas_2_temp PARTITIONED BY (value) AS
  SELECT key, value FROM src where key > 200 and key < 300;

EXPLAIN
SELECT * FROM partition_ctas_2_temp where value = 'val_238';

SELECT * FROM partition_ctas_2_temp where value = 'val_238';

EXPLAIN
SELECT value FROM partition_ctas_2_temp where key = 238;

SELECT value FROM partition_ctas_2_temp where key = 238;

CREATE TEMPORARY TABLE partition_ctas_diff_order_temp PARTITIONED BY (value) AS
  SELECT value, key FROM src where key > 200 and key < 300;

EXPLAIN
SELECT * FROM partition_ctas_diff_order_temp where value = 'val_238';

SELECT * FROM partition_ctas_diff_order_temp where value = 'val_238';

CREATE TEMPORARY TABLE partition_ctas_complex_order_temp PARTITIONED BY (c0, c4, c1) AS
  SELECT concat(value, '_0') as c0,
         concat(value, '_1') as c1,
         concat(value, '_2') as c2,
         concat(value, '_3') as c3,
         concat(value, '_5') as c5,
         concat(value, '_4') as c4
  FROM src where key > 200 and key < 240;

-- c2, c3, c5, c0, c4, c1
EXPLAIN
SELECT * FROM partition_ctas_complex_order_temp where c0 = 'val_238_0';

SELECT * FROM partition_ctas_complex_order_temp where c0 = 'val_238_0';
