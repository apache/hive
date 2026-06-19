DROP TABLE table1;
DROP TABLE table2;
DROP TABLE table3;

CREATE TABLE table1 (a STRING) ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.TypedBytesSerDe';
CREATE TABLE table2 (a STRING, b STRING) ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.TypedBytesSerDe';
CREATE TABLE table3 (a STRING, b STRING, c STRING) ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.TypedBytesSerDe';

DROP TABLE table1;
DROP TABLE table2;
DROP TABLE table3;
