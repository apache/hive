DROP TABLE t_ext_hbase_1;

CREATE EXTERNAL TABLE t_ext_hbase_1
(key STRING, c_bool BOOLEAN, c_byte TINYINT, c_short SMALLINT,
 c_int INT, c_long BIGINT, c_string STRING, c_float FLOAT, c_double DOUBLE)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,cf:cq-boolean,cf:cq-byte,cf:cq-short,cf:cq-int,cf:cq-long,cf:cq-string,cf:cq-float,cf:cq-double")
TBLPROPERTIES ("hbase.table.name" = "HiveExternalTable");

SELECT * FROM t_ext_hbase_1;

DROP TABLE t_ext_hbase_1;
DROP TABLE t_ext_hbase_2;

CREATE EXTERNAL TABLE t_ext_hbase_2
(key STRING, c_bool BOOLEAN, c_byte TINYINT, c_short SMALLINT,
 c_int INT, c_long BIGINT, c_string STRING, c_float FLOAT, c_double DOUBLE)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key#b,cf:cq-boolean#b,cf:cq-byte#b,cf:cq-short#b,cf:cq-int#b,cf:cq-long#b,cf:cq-string#b,cf:cq-float#b,cf:cq-double#b")
TBLPROPERTIES ("hbase.table.name" = "HiveExternalTable");

SELECT * FROM t_ext_hbase_2;

DROP TABLE t_ext_hbase_2;
DROP TABLE t_ext_hbase_3;

CREATE EXTERNAL TABLE t_ext_hbase_3
(key STRING, c_bool BOOLEAN, c_byte TINYINT, c_short SMALLINT,
 c_int INT, c_long BIGINT, c_string STRING, c_float FLOAT, c_double DOUBLE)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,cf:cq-boolean,cf:cq-byte,cf:cq-short,cf:cq-int,cf:cq-long,cf:cq-string,cf:cq-float,cf:cq-double")
TBLPROPERTIES (
"hbase.table.name" = "HiveExternalTable",
"hbase.table.default.storage.type" = "binary");

SELECT * from t_ext_hbase_3;

--HIVE-2958
SELECT c_int, count(*) FROM t_ext_hbase_3 GROUP BY c_int;

DROP table t_ext_hbase_3;
