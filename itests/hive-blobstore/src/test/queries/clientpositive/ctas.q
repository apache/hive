DROP TABLE IF EXISTS ctas_blobstore_table_src;
CREATE TABLE ctas_blobstore_table_src (col int) LOCATION '${hiveconf:test.blobstore.path.unique}/ctas_blobstore_table_src/';
INSERT INTO TABLE ctas_blobstore_table_src VALUES (1), (2), (3);

DROP TABLE IF EXISTS ctas_hdfs_table_src;
CREATE TABLE ctas_hdfs_table_src (col int);
INSERT INTO TABLE ctas_hdfs_table_src VALUES (1), (2), (3);

-- Test select from a Blobstore and write to HDFS
DROP TABLE IF EXISTS ctas_hdfs_table_dst;
EXPLAIN EXTENDED CREATE TABLE ctas_hdfs_table_dst AS SELECT * FROM ctas_blobstore_table_src;
CREATE TABLE ctas_hdfs_table_dst AS SELECT * FROM ctas_blobstore_table_src;
SELECT * FROM ctas_hdfs_table_dst;

-- Test select from HDFS and write to a Blobstore
DROP TABLE IF EXISTS ctas_blobstore_table_dst;
EXPLAIN EXTENDED CREATE TABLE ctas_blobstore_table_dst LOCATION '${hiveconf:test.blobstore.path.unique}/ctas_blobstore_table_dst/' AS SELECT * FROM ctas_hdfs_table_src;
CREATE TABLE ctas_blobstore_table_dst AS SELECT * FROM ctas_hdfs_table_src;
SELECT * FROM ctas_blobstore_table_dst;

-- Test select from a Blobstore and write to a Blobstore
DROP TABLE IF EXISTS ctas_blobstore_table_dst;
EXPLAIN EXTENDED CREATE TABLE ctas_blobstore_table_dst LOCATION '${hiveconf:test.blobstore.path.unique}/ctas_blobstore_table_dst/' AS SELECT * FROM ctas_blobstore_table_src;
CREATE TABLE ctas_blobstore_table_dst AS SELECT * FROM ctas_blobstore_table_src;
SELECT * FROM ctas_blobstore_table_dst;

DROP TABLE ctas_blobstore_table_dst;
DROP TABLE ctas_hdfs_table_dst;
DROP TABLE ctas_blobstore_table_src;
DROP TABLE ctas_hdfs_table_src;
