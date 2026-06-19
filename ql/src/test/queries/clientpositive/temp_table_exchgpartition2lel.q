--! qt:dataset:src
DROP TABLE IF EXISTS t1_n72_temp;
DROP TABLE IF EXISTS t2_n44_temp;
DROP TABLE IF EXISTS t3_n15_temp;
DROP TABLE IF EXISTS t4_n7_temp;

CREATE TEMPORARY TABLE t1_n72_temp (a int) PARTITIONED BY (d1 int);
CREATE TEMPORARY TABLE t2_n44_temp (a int) PARTITIONED BY (d1 int);
CREATE TEMPORARY TABLE t3_n15_temp (a int) PARTITIONED BY (d1 int, d2 int);
CREATE TEMPORARY TABLE t4_n7_temp (a int) PARTITIONED BY (d1 int, d2 int);
CREATE TEMPORARY TABLE t5_n3_temp (a int) PARTITIONED BY (d1 int, d2 int, d3 int);
CREATE TEMPORARY TABLE t6_n2_temp (a int) PARTITIONED BY (d1 int, d2 int, d3 int);
set hive.mapred.mode=nonstrict;
INSERT OVERWRITE TABLE t1_n72_temp PARTITION (d1 = 1) SELECT key FROM src where key = 100 limit 1;
INSERT OVERWRITE TABLE t3_n15_temp PARTITION (d1 = 1, d2 = 1) SELECT key FROM src where key = 100 limit 1;
INSERT OVERWRITE TABLE t5_n3_temp PARTITION (d1 = 1, d2 = 1, d3=1) SELECT key FROM src where key = 100 limit 1;

SELECT * FROM t1_n72_temp;

SELECT * FROM t3_n15_temp;

ALTER TABLE t2_n44_temp EXCHANGE PARTITION (d1 = 1) WITH TABLE t1_n72_temp;
SELECT * FROM t1_n72_temp;
SELECT * FROM t2_n44_temp;

ALTER TABLE t4_n7_temp EXCHANGE PARTITION (d1 = 1, d2 = 1) WITH TABLE t3_n15_temp;
SELECT * FROM t3_n15_temp;
SELECT * FROM t4_n7_temp;

EXPLAIN ALTER TABLE t6_n2_temp EXCHANGE PARTITION (d1 = 1, d2 = 1, d3 = 1) WITH TABLE t5_n3_temp;
ALTER TABLE t6_n2_temp EXCHANGE PARTITION (d1 = 1, d2 = 1, d3 = 1) WITH TABLE t5_n3_temp;
SELECT * FROM t5_n3_temp;
SELECT * FROM t6_n2_temp;

