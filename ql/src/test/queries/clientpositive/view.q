CREATE DATABASE db1;
USE db1;

CREATE TABLE table1_n19 (key STRING, value STRING)
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/kv1.txt'
OVERWRITE INTO TABLE table1_n19;

CREATE TABLE table2_n13 (key STRING, value STRING)
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/kv1.txt'
OVERWRITE INTO TABLE table2_n13;

-- relative reference, no alias
CREATE VIEW v1_n17 AS SELECT * FROM table1_n19;

-- relative reference, aliased
CREATE VIEW v2_n10 AS SELECT t1.* FROM table1_n19 t1;

-- relative reference, multiple tables
CREATE VIEW v3_n3 AS SELECT t1.*, t2.key k FROM table1_n19 t1 JOIN table2_n13 t2 ON t1.key = t2.key;

-- absolute reference, no alias
CREATE VIEW v4_n3 AS SELECT * FROM db1.table1_n19;

-- absolute reference, aliased
CREATE VIEW v5_n1 AS SELECT t1.* FROM db1.table1_n19 t1;

-- absolute reference, multiple tables
CREATE VIEW v6 AS SELECT t1.*, t2.key k FROM db1.table1_n19 t1 JOIN db1.table2_n13 t2 ON t1.key = t2.key;

-- relative reference, explicit column
CREATE VIEW v7 AS SELECT key from table1_n19;

-- absolute reference, explicit column
CREATE VIEW v8 AS SELECT key from db1.table1_n19;

CREATE DATABASE db2;
USE db2;

SELECT * FROM db1.v1_n17;
SELECT * FROM db1.v2_n10;
SELECT * FROM db1.v3_n3;
SELECT * FROM db1.v4_n3;
SELECT * FROM db1.v5_n1;
SELECT * FROM db1.v6;
SELECT * FROM db1.v7;
SELECT * FROM db1.v8;

