-- Create external table like should not result in data loss upon dropping the table
DROP TABLE blobstore_partitioned_source_table;
CREATE TABLE blobstore_partitioned_source_table (
  a int, b int, c string
)
PARTITIONED BY (dt int, hour int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ' '
COLLECTION ITEMS TERMINATED BY '\t'
LOCATION '${hiveconf:test.blobstore.path.unique}/create_like/data' ;
LOAD DATA LOCAL INPATH '../../data/files/test_data' OVERWRITE INTO TABLE blobstore_partitioned_source_table
PARTITION (dt=20110924, hour=1);
LOAD DATA LOCAL INPATH '../../data/files/test_data' OVERWRITE INTO TABLE blobstore_partitioned_source_table
PARTITION (dt=20110924, hour=2);
LOAD DATA LOCAL INPATH '../../data/files/test_data' OVERWRITE INTO TABLE blobstore_partitioned_source_table
PARTITION (dt=20110925, hour=1);
LOAD DATA LOCAL INPATH '../../data/files/test_data' OVERWRITE INTO TABLE blobstore_partitioned_source_table
PARTITION (dt=20110925, hour=2);

DROP TABLE like_table;
CREATE EXTERNAL TABLE like_table LIKE blobstore_partitioned_source_table LOCATION '${hiveconf:test.blobstore.path.unique}/create_like/data';

MSCK REPAIR TABLE like_table;

SELECT * FROM blobstore_partitioned_source_table;
SELECT * FROM like_table;

DROP TABLE like_table;

SELECT * FROM blobstore_partitioned_source_table;