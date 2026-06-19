-- Tests EXPLAIN INSERT OVERWRITE command

DROP TABLE blobstore_table;
CREATE TABLE blobstore_table (cnt INT)
LOCATION '${hiveconf:test.blobstore.path.unique}/explain/blobstore_table';
LOAD DATA LOCAL INPATH '../../data/files/single_int.txt' INTO TABLE blobstore_table;

SELECT * FROM blobstore_table;
EXPLAIN INSERT OVERWRITE TABLE blobstore_table SELECT count(1) FROM blobstore_table;
SELECT * FROM blobstore_table;
