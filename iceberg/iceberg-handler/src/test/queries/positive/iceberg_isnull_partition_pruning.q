CREATE TABLE sample01 (
  index INT,
  date_col DATE,
  timestamp_col TIMESTAMP,
  str_col VARCHAR(24),
  string_col STRING,
  double_col DOUBLE,
  float_col FLOAT,
  decimal_col DECIMAL(9,3),
  tinyint_col TINYINT,
  smallint_col SMALLINT,
  int_col INT,
  bigint_col BIGINT,
  boolean_col BOOLEAN
);

INSERT INTO sample01 VALUES
(1003,"1969-10-27","1993-05-17 07:39:58.375409",NULL,"sloppy bronze hare",-181.01933598375618,-181.019336,-999999.999,-128,-32768,-2147483648,-9223372036854775808,false);

CREATE EXTERNAL TABLE ice01 (
  index INT,
  date_col DATE,
  timestamp_col TIMESTAMP,
  string_col STRING,
  double_col DOUBLE,
  float_col FLOAT,
  decimal_col DECIMAL(9,3),
  smallint_col int,
  int_col INT,
  bigint_col BIGINT,
  boolean_col BOOLEAN
) PARTITIONED BY (str_col String, tinyint_col int)
STORED BY iceberg;

INSERT INTO ice01 PARTITION (str_col, tinyint_col)
  SELECT index, date_col, timestamp_col, string_col, double_col, float_col, decimal_col, smallint_col, int_col, bigint_col, boolean_col, str_col, tinyint_col FROM sample01;

SELECT * FROM ice01 WHERE str_col is NULL;