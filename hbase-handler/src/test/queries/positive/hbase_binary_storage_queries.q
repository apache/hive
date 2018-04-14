--! qt:dataset:src
DROP TABLE t_hbase;

CREATE EXTERNAL TABLE t_hbase(key STRING,
                     tinyint_col TINYINT,
                     smallint_col SMALLINT,
                     int_col INT,
                     bigint_col BIGINT,
                     float_col FLOAT,
                     double_col DOUBLE,
                     boolean_col BOOLEAN)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key#-,cf:binarybyte#-,cf:binaryshort#-,cf:binaryint#-,cf:binarylong#-,cf:binaryfloat#-,cf:binarydouble#-,cf:binaryboolean#-")
TBLPROPERTIES ("hbase.table.name" = "t_hive",
               "hbase.table.default.storage.type" = "binary",
               "external.table.purge" = "true");

DESCRIBE FORMATTED t_hbase;

INSERT OVERWRITE TABLE t_hbase
SELECT 'user1', 1, 1, 1, 1, 1.0, 1.0, true
FROM src
WHERE key=100 OR key=125 OR key=126;

INSERT OVERWRITE TABLE t_hbase
SELECT 'user2', 127, 32767, 2147483647, 9223372036854775807, 211.31, 268746532.0571, false
FROM src
WHERE key=100 OR key=125 OR key=126;

INSERT OVERWRITE TABLE t_hbase
SELECT 'user3', -128, -32768, -2147483648, -9223372036854775808, -201.17, -2110789.37145, true
FROM src
WHERE key=100 OR key=125 OR key=126;

SELECT * FROM t_hbase;

SELECT tinyint_col,
       smallint_col,
       int_col,
       bigint_col,
       float_col,
       double_col,
       boolean_col
FROM t_hbase
WHERE key='user1' OR key='user2' OR key='user3';

SELECT sum(tinyint_col),
       sum(smallint_col),
       sum(int_col),
       sum(bigint_col),
       sum(float_col),
       sum(double_col),
       count(boolean_col)
FROM t_hbase;

DROP TABLE t_hbase_1;

CREATE EXTERNAL TABLE t_hbase_1(key STRING,
                                tinyint_col TINYINT,
                                smallint_col SMALLINT,
                                int_col INT,
                                bigint_col BIGINT,
                                float_col FLOAT,
                                double_col DOUBLE,
                                boolean_col BOOLEAN)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key#b,cf:binarybyte#b,cf:binaryshort#b,cf:binaryint#b,cf:binarylong#b,cf:binaryfloat#b,cf:binarydouble#b,cf:binaryboolean#b")
TBLPROPERTIES ("hbase.table.name" = "t_hive");

DESCRIBE FORMATTED t_hbase_1;

SELECT * FROM t_hbase_1;

SELECT tinyint_col,
       smallint_col,
       int_col,
       bigint_col,
       float_col,
       double_col,
       boolean_col
FROM t_hbase_1
WHERE key='user1' OR key='user2' OR key='user3';

SELECT sum(tinyint_col),
       sum(smallint_col),
       sum(int_col),
       sum(bigint_col),
       sum(float_col),
       sum(double_col),
       count(boolean_col)
FROM t_hbase_1;

DROP TABLE t_hbase_1;
DROP TABLE t_hbase;
DROP TABLE t_hbase_2;

CREATE EXTERNAL TABLE t_hbase_2(key STRING,
                     tinyint_col TINYINT,
                     smallint_col SMALLINT,
                     int_col INT,
                     bigint_col BIGINT,
                     float_col FLOAT,
                     double_col DOUBLE,
                     boolean_col BOOLEAN)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key#-,cf:binarybyte#-,cf:binaryshort#-,cf:binaryint#-,cf:binarylong#-,cf:binaryfloat#-,cf:binarydouble#-,cf:binaryboolean#-")
TBLPROPERTIES ("hbase.table.name" = "t_hive_2", "external.table.purge" = "true");

INSERT OVERWRITE TABLE t_hbase_2
SELECT 'user1', 1, 1, 1, 1, 1.0, 1.0, true
FROM src
WHERE key=100 OR key=125 OR key=126;

INSERT OVERWRITE TABLE t_hbase_2
SELECT 'user2', 127, 32767, 2147483647, 9223372036854775807, 211.31, 268746532.0571, false
FROM src
WHERE key=100 OR key=125 OR key=126;

INSERT OVERWRITE TABLE t_hbase_2
SELECT 'user3', -128, -32768, -2147483648, -9223372036854775808, -201.17, -2110789.37145, true
FROM src
WHERE key=100 OR key=125 OR key=126;

SELECT * FROM t_hbase_2;

SELECT tinyint_col,
       smallint_col,
       int_col,
       bigint_col,
       float_col,
       double_col,
       boolean_col
FROM t_hbase_2
WHERE key='user1' OR key='user2' OR key='user3';

SELECT sum(tinyint_col),
       sum(smallint_col),
       sum(int_col),
       sum(bigint_col),
       sum(float_col),
       sum(double_col),
       count(boolean_col)
FROM t_hbase_2;

DROP TABLE t_hbase_3;

CREATE EXTERNAL TABLE t_hbase_3(key STRING,
                                tinyint_col TINYINT,
                                smallint_col SMALLINT,
                                int_col INT,
                                bigint_col BIGINT,
                                float_col FLOAT,
                                double_col DOUBLE,
                                boolean_col BOOLEAN)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key#b,cf:binarybyte#b,cf:binaryshort#b,cf:binaryint#b,cf:binarylong#b,cf:binaryfloat#b,cf:binarydouble#b,cf:binaryboolean#b")
TBLPROPERTIES ("hbase.table.name" = "t_hive_2");

SELECT * FROM t_hbase_3;

SELECT tinyint_col,
       smallint_col,
       int_col,
       bigint_col,
       float_col,
       double_col,
       boolean_col
FROM t_hbase_3
WHERE key='user1' OR key='user2' OR key='user3';

SELECT sum(tinyint_col),
       sum(smallint_col),
       sum(int_col),
       sum(bigint_col),
       sum(float_col),
       sum(double_col),
       count(boolean_col)
FROM t_hbase_3;

DROP TABLE t_hbase_3;

DROP TABLE t_hbase_4;

CREATE EXTERNAL TABLE t_hbase_4(key STRING,
                     tinyint_col TINYINT,
                     smallint_col SMALLINT,
                     int_col INT,
                     bigint_col BIGINT,
                     float_col FLOAT,
                     double_col DOUBLE,
                     boolean_col BOOLEAN)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key#-,cf:binarybyte#-,cf:binaryshort#-,cf:binaryint#-,cf:binarylong#-,cf:binaryfloat#-,cf:binarydouble#-,cf:binaryboolean#-")
TBLPROPERTIES (
"hbase.table.name" = "t_hive_2",
"hbase.table.default.storage.type" = "binary",
"external.table.purge" = "true");

SELECT * FROM t_hbase_4;

SELECT tinyint_col,
       smallint_col,
       int_col,
       bigint_col,
       float_col,
       double_col,
       boolean_col
FROM t_hbase_4
WHERE key='user1' OR key='user2' OR key='user3';

SELECT sum(tinyint_col),
       sum(smallint_col),
       sum(int_col),
       sum(bigint_col),
       sum(float_col),
       sum(double_col),
       count(boolean_col)
FROM t_hbase_4;

DROP TABLE t_hbase_4;
DROP TABLE t_hbase_2;
