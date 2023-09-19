-- in this table, rows of different lengths(different number of columns) are loaded
CREATE TABLE t1_multi_delimit(colA int,
  colB tinyint,
  colC timestamp,
  colD smallint,
  colE smallint)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.MultiDelimitSerDe'
WITH SERDEPROPERTIES ("field.delim"="^,")STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH "../../data/files/t1_multi_delimit.csv" INTO TABLE t1_multi_delimit;

SELECT * FROM t1_multi_delimit;

-- in this table, rows of different lengths(different number of columns) and it uses csv serde
CREATE TABLE t11_csv_serde(colA int,
  colB tinyint,
  colC timestamp,
  colD smallint,
  colE smallint)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ("separatorChar" = ",")STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH "../../data/files/t11_csv_serde.csv" INTO TABLE t11_csv_serde;

SELECT * FROM t11_csv_serde;

-- there should not be any difference between MultiDelimitSerDe table and OpenCSVSerde table results

SELECT EXISTS (
SELECT colA, colB, colC, colD, colE FROM t1_multi_delimit
MINUS
SELECT cast(colA as int), cast(colB as tinyint), cast(colC as timestamp), cast(colD as smallint), cast(colE as smallint) FROM t11_csv_serde
);

-- in this table, file having extra column is loaded
CREATE TABLE t2_multi_delimit(colA int,
  colB tinyint,
  colC timestamp,
  colD smallint,
  colE smallint)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.MultiDelimitSerDe'
WITH SERDEPROPERTIES ("field.delim"="^,")STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH "../../data/files/t2_multi_delimit.csv" INTO TABLE t2_multi_delimit;

SELECT * FROM t2_multi_delimit;

-- in this table, delimiter of 5 characters is used
CREATE TABLE t3_multi_delimit(colA int,
  colB tinyint,
  colC timestamp,
  colD smallint,
  colE smallint)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.MultiDelimitSerDe'
WITH SERDEPROPERTIES ("field.delim"="^^^^^")STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH "../../data/files/t3_multi_delimit.csv" INTO TABLE t3_multi_delimit;

SELECT * FROM t3_multi_delimit;


DROP TABLE t1_multi_delimit;
DROP TABLE t11_csv_serde;
DROP TABLE t2_multi_delimit;
DROP TABLE t3_multi_delimit;