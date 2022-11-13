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

select trunc(CAST('123456789.123456789' AS DECIMAL(18,9)), 8);
select trunc(CAST('123456789.123456789' AS DECIMAL(18,9)), 7);
select trunc(CAST('123456789.123456789' AS DECIMAL(18,9)), 6);
select trunc(CAST('123456789.123456789' AS DECIMAL(18,9)), 5);
select trunc(CAST('123456789.123456789' AS DECIMAL(18,9)), 4);
select trunc(CAST('123456789.123456789' AS DECIMAL(18,9)), 3);
select trunc(CAST('123456789.123456789' AS DECIMAL(18,9)), 2);
select trunc(CAST('123456789.123456789' AS DECIMAL(18,9)), 1);
select trunc(CAST('123456789.123456789' AS DECIMAL(18,9)), 0);
select trunc(CAST('123456789.123456789' AS DECIMAL(18,9)), -1);
select trunc(CAST('123456789.123456789' AS DECIMAL(18,9)), -2);
select trunc(CAST('123456789.123456789' AS DECIMAL(18,9)), -3);
select trunc(CAST('123456789.123456789' AS DECIMAL(18,9)), -4);
select trunc(CAST('123456789.123456789' AS DECIMAL(18,9)), -5);
select trunc(CAST('123456789.123456789' AS DECIMAL(18,9)), -6);
select trunc(CAST('123456789.123456789' AS DECIMAL(18,9)), -7);
select trunc(CAST('123456789.123456789' AS DECIMAL(18,9)), -8);


-- trunc double
select trunc(CAST('123456789.123456789' AS DOUBLE), 8);
select trunc(CAST('123456789.123456789' AS DOUBLE), 7);
select trunc(CAST('123456789.123456789' AS DOUBLE), 6);
select trunc(CAST('123456789.123456789' AS DOUBLE), 5);
select trunc(CAST('123456789.123456789' AS DOUBLE), 4);
select trunc(CAST('123456789.123456789' AS DOUBLE), 3);
select trunc(CAST('123456789.123456789' AS DOUBLE), 2);
select trunc(CAST('123456789.123456789' AS DOUBLE), 1);
select trunc(CAST('123456789.123456789' AS DOUBLE), 0);
select trunc(CAST('123456789.123456789' AS DOUBLE), -1);
select trunc(CAST('123456789.123456789' AS DOUBLE), -2);
select trunc(CAST('123456789.123456789' AS DOUBLE), -3);
select trunc(CAST('123456789.123456789' AS DOUBLE), -4);
select trunc(CAST('123456789.123456789' AS DOUBLE), -5);
select trunc(CAST('123456789.123456789' AS DOUBLE), -6);
select trunc(CAST('123456789.123456789' AS DOUBLE), -7);
select trunc(CAST('123456789.123456789' AS DOUBLE), -8);

-- trunc float
select trunc(CAST('1234567.1' AS FLOAT), 1);
select trunc(CAST('1234567.1' AS FLOAT), 0);
select trunc(CAST('1234567.1' AS FLOAT));
select trunc(CAST('1234567.1' AS FLOAT), -1);
select trunc(CAST('1234567.1' AS FLOAT), -2);
select trunc(CAST('1234567.1' AS FLOAT), -3);
select trunc(CAST('1234567.1' AS FLOAT), -4);
select trunc(CAST('1234567.1' AS FLOAT), -5);
select trunc(CAST('1234567.1' AS FLOAT), -6);
select trunc(CAST('1234567.1' AS FLOAT), -7);
select trunc(CAST('1234567.1' AS FLOAT), -8);

-- trunc longCAST('
select trunc(CAST('123456789' AS BIGINT), 2);
select trunc(CAST('123456789' AS BIGINT), 2);
select trunc(CAST('123456789' AS BIGINT), 1);
select trunc(CAST('123456789' AS BIGINT), 0);
select trunc(CAST('123456789' AS BIGINT), -1);
select trunc(CAST('123456789' AS BIGINT), -2);
select trunc(CAST('123456789' AS BIGINT), -3);
select trunc(CAST('123456789' AS BIGINT), -4);
select trunc(CAST('123456789' AS BIGINT), -5);
select trunc(CAST('123456789' AS BIGINT), -6);
select trunc(CAST('123456789' AS BIGINT), -7);
select trunc(CAST('123456789' AS BIGINT), -8);


-- trunc Int
select trunc(CAST('123456789' AS INT), 3);
select trunc(CAST('123456789' AS INT), 2);
select trunc(CAST('123456789' AS INT), 1);
select trunc(CAST('123456789' AS INT), 0);
select trunc(CAST('123456789' AS INT), -1);
select trunc(CAST('123456789' AS INT), -2);
select trunc(CAST('123456789' AS INT), -3);
select trunc(CAST('123456789' AS INT), -4);
select trunc(CAST('123456789' AS INT), -5);
select trunc(CAST('123456789' AS INT), -6);
select trunc(CAST('123456789' AS INT), -7);
select trunc(CAST('123456789' AS INT), -8);