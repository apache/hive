--! qt:dataset:src
set hive.fetch.task.conversion=more;

EXPLAIN SELECT trunc(1234567891.1234567891,4), trunc(1234567891.1234567891,-4), trunc(1234567891.1234567891,0), trunc(1234567891.1234567891) FROM src tablesample (1 rows);

SELECT trunc(1234567891.1234567891,4), trunc(1234567891.1234567891,-4), trunc(1234567891.1234567891,0), trunc(1234567891.1234567891) FROM src tablesample (1 rows);

SELECT trunc(12.123891,4), trunc(12,-4) FROM src tablesample (1 rows);
DROP TABLE sampletable;

CREATE TABLE sampletable(c DOUBLE, d INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/trunc_number.txt' INTO TABLE sampletable;

EXPLAIN select trunc (c,d) from sampletable;

select trunc (c,d) from sampletable;

CREATE TABLE sampletable1(c FLOAT, d SMALLINT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/trunc_number.txt' INTO TABLE sampletable1;

EXPLAIN select trunc (c,d) from sampletable1;

select trunc (c,d) from sampletable1;

CREATE TABLE sampletable2(c DOUBLE, d BIGINT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/trunc_number.txt' INTO TABLE sampletable2;

EXPLAIN select trunc (c,d) from sampletable2;

select trunc (c,d) from sampletable2;

CREATE TABLE sampletable3(c BIGINT, d SMALLINT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/trunc_number1.txt' INTO TABLE sampletable3;

EXPLAIN select trunc (c,d) from sampletable3;

select trunc (c,d) from sampletable3;

CREATE TABLE sampletable4(c INT, d INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/trunc_number1.txt' INTO TABLE sampletable4;

EXPLAIN select trunc (c,d) from sampletable4;

select trunc (c,d) from sampletable4;

DROP TABLE sampletable;

DROP TABLE sampletable1;

DROP TABLE sampletable2;

DROP TABLE sampletable3;

DROP TABLE sampletable4;