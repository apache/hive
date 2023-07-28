set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;
SET hive.auto.convert.join=true;
SET hive.auto.convert.join.noconditionaltask=true;
SET hive.auto.convert.join.noconditionaltask.size=1000000000;

-- SORT_QUERY_RESULTS

CREATE TABLE table_8_txt (tinyint_col_1 TINYINT, float_col_2 FLOAT, bigint_col_3 BIGINT, boolean_col_4 BOOLEAN, decimal0202_col_5 DECIMAL(2, 2), decimal1612_col_6 DECIMAL(16, 12), double_col_7 DOUBLE, char0205_col_8 CHAR(205), bigint_col_9 BIGINT, decimal1202_col_10 DECIMAL(12, 2), boolean_col_11 BOOLEAN, double_col_12 DOUBLE, decimal2208_col_13 DECIMAL(22, 8), decimal3722_col_14 DECIMAL(37, 22), smallint_col_15 SMALLINT, decimal2824_col_16 DECIMAL(28, 24), boolean_col_17 BOOLEAN, float_col_18 FLOAT, timestamp_col_19 TIMESTAMP, decimal0402_col_20 DECIMAL(4, 2), char0208_col_21 CHAR(208), char0077_col_22 CHAR(77), decimal2915_col_23 DECIMAL(29, 15), char0234_col_24 CHAR(234), timestamp_col_25 TIMESTAMP, tinyint_col_26 TINYINT, decimal3635_col_27 DECIMAL(36, 35), boolean_col_28 BOOLEAN, float_col_29 FLOAT, smallint_col_30 SMALLINT, varchar0200_col_31 VARCHAR(200), boolean_col_32 BOOLEAN)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001';

LOAD DATA LOCAL INPATH '../../data/files/table_8.dat' INTO TABLE table_8_txt;

CREATE TABLE table_8
STORED AS orc AS
SELECT * FROM table_8_txt;

analyze table table_8 compute statistics;


CREATE EXTERNAL TABLE table_19_txt (float_col_1 FLOAT, varchar0037_col_2 VARCHAR(37), decimal2912_col_3 DECIMAL(29, 12), decimal0801_col_4 DECIMAL(8, 1), timestamp_col_5 TIMESTAMP, boolean_col_6 BOOLEAN, string_col_7 STRING, tinyint_col_8 TINYINT, boolean_col_9 BOOLEAN, decimal1614_col_10 DECIMAL(16, 14), boolean_col_11 BOOLEAN, float_col_12 FLOAT, char0116_col_13 CHAR(116), boolean_col_14 BOOLEAN, string_col_15 STRING, double_col_16 DOUBLE, string_col_17 STRING, bigint_col_18 BIGINT, int_col_19 INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001';

LOAD DATA LOCAL INPATH '../../data/files/table_19.dat' INTO TABLE table_19_txt;

CREATE TABLE table_19
STORED AS orc AS
SELECT * FROM table_19_txt;

analyze table table_19 compute statistics;


EXPLAIN VECTORIZATION DETAIL SELECT count(t2.smallint_col_15) FROM   table_19 t1  INNER JOIN table_8 t2 ON t2.decimal0402_col_20 = t1.decimal0801_col_4;
SELECT count(t2.smallint_col_15) FROM   table_19 t1  INNER JOIN table_8 t2 ON t2.decimal0402_col_20 = t1.decimal0801_col_4;

SET hive.vectorized.input.format.supports.enabled="none";
EXPLAIN VECTORIZATION DETAIL SELECT count(t2.smallint_col_15) FROM   table_19 t1  INNER JOIN table_8 t2 ON t2.decimal0402_col_20 = t1.decimal0801_col_4;
SELECT count(t2.smallint_col_15) FROM   table_19 t1  INNER JOIN table_8 t2 ON t2.decimal0402_col_20 = t1.decimal0801_col_4;
