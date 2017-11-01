-- Query spanning multiple databases

DROP DATABASE IF EXISTS db1;
DROP DATABASE IF EXISTS db2;

CREATE DATABASE db1 LOCATION '${hiveconf:test.blobstore.path.unique}/multiple_db/db1';
CREATE DATABASE db2 LOCATION '${hiveconf:test.blobstore.path.unique}/multiple_db/db2';

USE db1;

DROP TABLE test_db1_tbl;
CREATE TABLE test_db1_tbl (
    a INT,
    b STRING,
    value DOUBLE)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ' '
LINES TERMINATED BY '\n';
LOAD DATA LOCAL INPATH '../../data/files/3col_data.txt' INTO TABLE test_db1_tbl;

USE db2;

DROP TABLE test_db2_tbl;
CREATE TABLE test_db2_tbl (a INT);
LOAD DATA LOCAL INPATH '../../data/files/single_int.txt' INTO TABLE test_db2_tbl;

SELECT * FROM test_db2_tbl;

SELECT * FROM db1.test_db1_tbl;

DROP TABLE test_db2_tbl;

USE db1;

DROP TABLE test_db1_tbl;