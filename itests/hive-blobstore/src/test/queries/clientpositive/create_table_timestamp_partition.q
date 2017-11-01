set hive.exec.dynamic.partition.mode=nonstrict;

DROP TABLE blobstore_partitioned_source_table;
DROP TABLE blobstore_partitioned_target_table;

CREATE EXTERNAL TABLE blobstore_partitioned_source_table (
  tsbucket TIMESTAMP,geo_country STRING,aid BIGINT)
STORED AS ORC
LOCATION '${hiveconf:test.blobstore.path.unique}/source_table/data'
TBLPROPERTIES("orc.compress"="ZLIB");

INSERT INTO TABLE blobstore_partitioned_source_table VALUES
  ('2016-11-02 17:00:00','France',74530),
  ('2016-11-02 18:00:00','Canada',57008),
  ('2016-11-02 17:00:00','Morocco',58097);

CREATE EXTERNAL TABLE blobstore_partitioned_target_table (
  geo_country STRING,aid BIGINT)
PARTITIONED BY (tsbucket TIMESTAMP)
STORED AS ORC
LOCATION '${hiveconf:test.blobstore.path.unique}/target_table/data'
TBLPROPERTIES("orc.compress"="ZLIB");

INSERT INTO TABLE blobstore_partitioned_target_table PARTITION (tsbucket)
SELECT geo_country,aid,tsbucket FROM blobstore_partitioned_source_table;

SHOW PARTITIONS blobstore_partitioned_target_table;

DESCRIBE formatted blobstore_partitioned_target_table PARTITION (tsbucket='2016-11-02 17:00:00');

DESCRIBE formatted blobstore_partitioned_target_table PARTITION (tsbucket='2016-11-02 17:00:00.0');

DROP TABLE blobstore_partitioned_source_table;
DROP TABLE blobstore_partitioned_target_table;
