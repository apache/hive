-- Insert overwrite into blobstore when WHERE clause returns zero rows
DROP TABLE blobstore_source;
CREATE TABLE blobstore_source (
    key int
) 
LOCATION '${hiveconf:test.blobstore.path.unique}/zero_rows_blobstore/blobstore_source/';
LOAD DATA LOCAL INPATH '../../data/files/kv6.txt' INTO TABLE blobstore_source;

DROP TABLE blobstore_target;
CREATE TABLE blobstore_target (
    key int
) 
LOCATION '${hiveconf:test.blobstore.path.unique}/zero_rows_blobstore/blobstore_target';

SELECT COUNT(*) FROM blobstore_target;
INSERT OVERWRITE TABLE blobstore_target SELECT key FROM blobstore_source;
SELECT COUNT(*) FROM blobstore_target;
INSERT OVERWRITE TABLE blobstore_target SELECT key FROM blobstore_source WHERE FALSE;
SELECT COUNT(*) FROM blobstore_target;
