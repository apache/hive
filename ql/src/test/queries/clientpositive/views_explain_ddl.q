set hive.cbo.enable = True;
set hive.vectorized.execution.enabled = True;

CREATE DATABASE db1;
USE db1;

CREATE TABLE table1_n19 (key STRING, value STRING)
STORED AS TEXTFILE;

CREATE TABLE table2_n13 (key STRING, value STRING)
STORED AS TEXTFILE;

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

explain ddl select  * FROM db1.v1_n17;
explain ddl select  * FROM db1.v2_n10;
explain ddl select  * FROM db1.v3_n3;
explain ddl select  * FROM db1.v4_n3;
explain ddl select  * FROM db1.v5_n1;
explain ddl select  * FROM db1.v6;
explain ddl select  * FROM db1.v7;
explain ddl select  * FROM db1.v8;

use default;
drop database db1 cascade;
drop database db2 cascade;