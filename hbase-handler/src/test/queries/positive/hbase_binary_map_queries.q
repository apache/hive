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
                          tinyint_map_col MAP<TINYINT, TINYINT>,
                          smallint_map_col MAP<SMALLINT, SMALLINT>,
                          int_map_col MAP<INT, INT>,
                          bigint_map_col MAP<BIGINT, BIGINT>,
                          float_map_col MAP<FLOAT, FLOAT>,
                          double_map_col MAP<DOUBLE, DOUBLE>,
                          string_map_col MAP<STRING, STRING>,
                          boolean_map_col MAP<BOOLEAN, BOOLEAN>)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping"=":key,cf-tinyint:,cf-smallint:,cf-int:,cf-bigint:,cf-float:,cf-double:,cf-string:,cf-boolean:")
TBLPROPERTIES ("hbase.table.name"="t_hive_maps");

INSERT OVERWRITE TABLE t_hbase_maps
  SELECT key,
         map(tinyint_col, tinyint_col),
         map(smallint_col, smallint_col),
         map(int_col, int_col),
         map(bigint_col, bigint_col),
         map(float_col, float_col),
         map(double_col, double_col),
         map(key, string_col),
         map(true, true)
  FROM hbase_src
  WHERE key = 125;

INSERT OVERWRITE TABLE t_hbase_maps
  SELECT key,
         map(tinyint_col, tinyint_col),
         map(smallint_col, smallint_col),
         map(int_col, int_col),
         map(bigint_col, bigint_col),
         map(float_col, float_col),
         map(double_col, double_col),
         map(key, string_col),
         map(false, false)
  FROM hbase_src
  WHERE key = 126;

SELECT * FROM t_hbase_maps ORDER BY key;

DROP TABLE t_ext_hbase_maps;

CREATE EXTERNAL TABLE t_ext_hbase_maps(key STRING,
                                       tinyint_map_col MAP<TINYINT, TINYINT>,
                                       smallint_map_col MAP<SMALLINT, SMALLINT>,
                                       int_map_col MAP<INT, INT>,
                                       bigint_map_col MAP<BIGINT, BIGINT>,
                                       float_map_col MAP<FLOAT, FLOAT>,
                                       double_map_col MAP<DOUBLE, DOUBLE>,
                                       string_map_col MAP<STRING, STRING>,
                                       boolean_map_col MAP<BOOLEAN, BOOLEAN>)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping"=":key,cf-tinyint:,cf-smallint:,cf-int:,cf-bigint:,cf-float:,cf-double:,cf-string:,cf-boolean:")
TBLPROPERTIES ("hbase.table.name"="t_hive_maps");

SELECT * FROM t_ext_hbase_maps ORDER BY key;

DROP TABLE t_ext_hbase_maps;

DROP TABLE t_ext_hbase_maps_1;

CREATE EXTERNAL TABLE t_ext_hbase_maps_1(key STRING,
                                         tinyint_map_col MAP<TINYINT, TINYINT>,
                                         smallint_map_col MAP<SMALLINT, SMALLINT>,
                                         int_map_col MAP<INT, INT>,
                                         bigint_map_col MAP<BIGINT, BIGINT>,
                                         float_map_col MAP<FLOAT, FLOAT>,
                                         double_map_col MAP<DOUBLE, DOUBLE>,
                                         string_map_col MAP<STRING, STRING>,
                                         boolean_map_col MAP<BOOLEAN, BOOLEAN>)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping"=":key#b,cf-tinyint:#bi:bi,cf-smallint:#bin:bin,cf-int:#bina:bina,cf-bigint:#binar:binar,cf-float:#binary:binary,cf-double:#b:b,cf-string:#bi:bi,cf-boolean:#bin:bin")
TBLPROPERTIES ("hbase.table.name"="t_hive_maps");

SELECT * FROM t_ext_hbase_maps_1 ORDER BY key;

DROP TABLE t_ext_hbase_maps_1;

DROP TABLE t_ext_hbase_maps_2;

CREATE EXTERNAL TABLE t_ext_hbase_maps_2(key STRING,
                                         tinyint_map_col MAP<TINYINT, TINYINT>,
                                         smallint_map_col MAP<SMALLINT, SMALLINT>,
                                         int_map_col MAP<INT, INT>,
                                         bigint_map_col MAP<BIGINT, BIGINT>,
                                         float_map_col MAP<FLOAT, FLOAT>,
                                         double_map_col MAP<DOUBLE, DOUBLE>,
                                         string_map_col MAP<STRING, STRING>,
                                         boolean_map_col MAP<BOOLEAN, BOOLEAN>)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping"=":key,cf-tinyint:,cf-smallint:,cf-int:,cf-bigint:,cf-float:,cf-double:,cf-string:,cf-boolean:")
TBLPROPERTIES (
"hbase.table.name"="t_hive_maps",
"hbase.table.default.storage.type"="binary");

SELECT * FROM t_ext_hbase_maps_2 ORDER BY key;

DROP TABLE t_ext_hbase_maps_2;

DROP TABLE t_hbase_maps_1;

CREATE TABLE t_hbase_maps_1(key STRING,
                            tinyint_map_col MAP<TINYINT, TINYINT>,
                            smallint_map_col MAP<SMALLINT, SMALLINT>,
                            int_map_col MAP<INT, INT>,
                            bigint_map_col MAP<BIGINT, BIGINT>,
                            float_map_col MAP<FLOAT, FLOAT>,
                            double_map_col MAP<DOUBLE, DOUBLE>,
                            string_map_col MAP<STRING, STRING>,
                            boolean_map_col MAP<BOOLEAN, BOOLEAN>)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping"=":key#b,cf-tinyint:#b:b,cf-smallint:#b:b,cf-int:#b:b,cf-bigint:#b:b,cf-float:#b:b,cf-double:#b:b,cf-string:#b:b,cf-boolean:#b:b")
TBLPROPERTIES ("hbase.table.name"="t_hive_maps_1");

INSERT OVERWRITE TABLE t_hbase_maps_1
  SELECT key,
         map(tinyint_col, tinyint_col),
         map(smallint_col, smallint_col),
         map(int_col, int_col),
         map(bigint_col, bigint_col),
         map(float_col, float_col),
         map(double_col, double_col),
         map(key, string_col),
         map(true, true)
  FROM hbase_src
  WHERE key = 125;

INSERT OVERWRITE TABLE t_hbase_maps_1
  SELECT key,
         map(tinyint_col, tinyint_col),
         map(smallint_col, smallint_col),
         map(int_col, int_col),
         map(bigint_col, bigint_col),
         map(float_col, float_col),
         map(double_col, double_col),
         map(key, string_col),
         map(false, false)
  FROM hbase_src
  WHERE key = 126;

SELECT * FROM t_hbase_maps_1 ORDER BY key;

DROP TABLE t_ext_hbase_maps_3;

CREATE EXTERNAL TABLE t_ext_hbase_maps_3(key STRING,
                                         tinyint_map_col MAP<TINYINT, TINYINT>,
                                         smallint_map_col MAP<SMALLINT, SMALLINT>,
                                         int_map_col MAP<INT, INT>,
                                         bigint_map_col MAP<BIGINT, BIGINT>,
                                         float_map_col MAP<FLOAT, FLOAT>,
                                         double_map_col MAP<DOUBLE, DOUBLE>,
                                         string_map_col MAP<STRING, STRING>,
                                         boolean_map_col MAP<BOOLEAN, BOOLEAN>)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping"=":key#b,cf-tinyint:#bi:bi,cf-smallint:#bin:bin,cf-int:#bina:bina,cf-bigint:#binar:binar,cf-float:#binary:binary,cf-double:#b:b,cf-string:#bi:bi,cf-boolean:#bin:bin")
TBLPROPERTIES ("hbase.table.name"="t_hive_maps_1");

SELECT * FROM t_ext_hbase_maps_3 ORDER BY key;

DROP TABLE t_ext_hbase_maps_3;

DROP TABLE t_ext_hbase_maps_4;

CREATE EXTERNAL TABLE t_ext_hbase_maps_4(key STRING,
                                         tinyint_map_col MAP<TINYINT, TINYINT>,
                                         smallint_map_col MAP<SMALLINT, SMALLINT>,
                                         int_map_col MAP<INT, INT>,
                                         bigint_map_col MAP<BIGINT, BIGINT>,
                                         float_map_col MAP<FLOAT, FLOAT>,
                                         double_map_col MAP<DOUBLE, DOUBLE>,
                                         string_map_col MAP<STRING, STRING>,
                                         boolean_map_col MAP<BOOLEAN, BOOLEAN>)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping"=":key,cf-tinyint:,cf-smallint:,cf-int:,cf-bigint:,cf-float:,cf-double:,cf-string:,cf-boolean:")
TBLPROPERTIES ("hbase.table.name"="t_hive_maps_1");

SELECT * FROM t_ext_hbase_maps_4 ORDER BY key;

DROP TABLE t_ext_hbase_maps_4;

DROP TABLE t_ext_hbase_maps_5;

CREATE EXTERNAL TABLE t_ext_hbase_maps_5(key STRING,
                                         tinyint_map_col MAP<TINYINT, TINYINT>,
                                         smallint_map_col MAP<SMALLINT, SMALLINT>,
                                         int_map_col MAP<INT, INT>,
                                         bigint_map_col MAP<BIGINT, BIGINT>,
                                         float_map_col MAP<FLOAT, FLOAT>,
                                         double_map_col MAP<DOUBLE, DOUBLE>,
                                         string_map_col MAP<STRING, STRING>,
                                         boolean_map_col MAP<BOOLEAN, BOOLEAN>)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping"=":key,cf-tinyint:,cf-smallint:,cf-int:,cf-bigint:,cf-float:,cf-double:,cf-string:,cf-boolean:")
TBLPROPERTIES (
"hbase.table.name"="t_hive_maps_1",
"hbase.table.default.storage.type"="binary");

SELECT * FROM t_ext_hbase_maps_5 ORDER BY key;

DROP TABLE t_ext_hbase_maps_5;

DROP TABLE t_hbase_maps_1;

DROP TABLE t_hbase_maps;

DROP TABLE hbase_src;
