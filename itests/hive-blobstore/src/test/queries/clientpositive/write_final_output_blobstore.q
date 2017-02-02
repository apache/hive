-- Test that the when multiple MR jobs are created for a query, that only the FSOP from the last job writes to S3

-- Drop tables
DROP TABLE IF EXISTS hdfs_table;
DROP TABLE IF EXISTS blobstore_table;

-- Create a table one table on HDFS and another on S3
CREATE TABLE hdfs_table(key INT);
CREATE TABLE blobstore_table(key INT) LOCATION '${hiveconf:test.blobstore.path.unique}/write_final_output_blobstore/';

SET hive.blobstore.use.blobstore.as.scratchdir=false;

SET hive.blobstore.optimizations.enabled=false;
EXPLAIN EXTENDED FROM hdfs_table INSERT OVERWRITE TABLE blobstore_table SELECT hdfs_table.key GROUP BY hdfs_table.key ORDER BY hdfs_table.key;

SET hive.blobstore.optimizations.enabled=true;
EXPLAIN EXTENDED FROM hdfs_table INSERT OVERWRITE TABLE blobstore_table SELECT hdfs_table.key GROUP BY hdfs_table.key ORDER BY hdfs_table.key;

-- Drop tables
DROP TABLE hdfs_table;
DROP TABLE blobstore_table;
