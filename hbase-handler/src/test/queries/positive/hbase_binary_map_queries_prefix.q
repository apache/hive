DROP TABLE hbase_src;

CREATE TABLE hbase_src(key STRING,
                       tinyint_col TINYINT,
                       smallint_col SMALLINT,
                       int_col INT,
                       bigint_col BIGINT,
                       float_col FLOAT,
                       double_col DOUBLE,
                       string_col STRING);

INSERT OVERWRITE TABLE hbase_src
  SELECT key, key, key, key, key, key, key, value
  FROM src
  WHERE key = 125 OR key = 126 OR key = 127;

DROP TABLE t_hbase_maps;

CREATE TABLE t_hbase_maps(key STRING,
                          string_map_col MAP<STRING, STRING>,
                          simple_string_col STRING)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping"=":key,cf-string:,cf-string:simple_string_col")
TBLPROPERTIES ("hbase.table.name"="t_hive_maps");

INSERT OVERWRITE TABLE t_hbase_maps
  SELECT key,
         map("string_col", string_col),
         string_col
  FROM hbase_src
  WHERE key = 125;

INSERT OVERWRITE TABLE t_hbase_maps
  SELECT key,
         map("string_col", string_col),
         string_col
  FROM hbase_src
  WHERE key = 126;

SELECT * FROM t_hbase_maps ORDER BY key;

DROP TABLE t_ext_hbase_maps;

CREATE EXTERNAL TABLE t_ext_hbase_maps(key STRING,
                                       string_map_cols MAP<STRING, STRING>, simple_string_col STRING)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping"=":key,cf-string:string_col.*,cf-string:simple_string_col")
TBLPROPERTIES ("hbase.table.name"="t_hive_maps");

SELECT * FROM t_ext_hbase_maps ORDER BY key;

DROP TABLE t_ext_hbase_maps;