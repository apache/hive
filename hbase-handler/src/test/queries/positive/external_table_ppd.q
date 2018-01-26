DROP TABLE t_hbase;

CREATE TABLE t_hbase(key STRING,
                     tinyint_col TINYINT,
                     smallint_col SMALLINT,
                     int_col INT,
                     bigint_col BIGINT,
                     float_col FLOAT,
                     double_col DOUBLE,
                     boolean_col BOOLEAN)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = "cf:binarykey#-,cf:binarybyte#-,cf:binaryshort#-,:key#-,cf:binarylong#-,cf:binaryfloat#-,cf:binarydouble#-,cf:binaryboolean#-")
TBLPROPERTIES ("hbase.table.name" = "t_hive",
               "hbase.table.default.storage.type" = "binary");

DESCRIBE FORMATTED t_hbase;

INSERT OVERWRITE TABLE t_hbase
SELECT 'user1', 1, 11, 10, 1, 1.0, 1.0, true
FROM src
WHERE key=100 OR key=125 OR key=126;

INSERT OVERWRITE TABLE t_hbase
SELECT 'user2', 127, 327, 2147, 9223372036854775807, 211.31, 268746532.0571, false
FROM src
WHERE key=100 OR key=125 OR key=126;

INSERT OVERWRITE TABLE t_hbase
SELECT 'user3', -128, -327, -214748, -9223372036854775808, -201.17, -2110789.37145, true
FROM src
WHERE key=100 OR key=125 OR key=126;

explain SELECT * FROM t_hbase where int_col > 0;
SELECT * FROM t_hbase where int_col > 0;

DROP TABLE t_hbase;

