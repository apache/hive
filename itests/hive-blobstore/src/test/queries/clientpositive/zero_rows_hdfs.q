-- Insert overwrite into hdfs from blobstore when WHERE clause returns zero rows
DROP TABLE blobstore_source;
CREATE TABLE blobstore_source (
    key int
)
LOCATION '${hiveconf:test.blobstore.path.unique}/zero_rows_hdfs/blobstore_source/';
LOAD DATA LOCAL INPATH '../../data/files/kv6.txt' INTO TABLE blobstore_source;

DROP TABLE hdfs_target;
CREATE TABLE hdfs_target (
    key int
);

SELECT COUNT(*) FROM hdfs_target;
INSERT OVERWRITE TABLE hdfs_target SELECT key FROM blobstore_source;
SELECT COUNT(*) FROM hdfs_target;
INSERT OVERWRITE TABLE hdfs_target SELECT key FROM blobstore_source WHERE FALSE;
SELECT COUNT(*) FROM hdfs_target;
