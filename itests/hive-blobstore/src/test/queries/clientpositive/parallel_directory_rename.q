SET hive.blobstore.optimizations.enabled=true;
SET hive.blobstore.use.blobstore.as.scratchdir=true;

DROP TABLE parallel_directory_rename;
CREATE TABLE parallel_directory_rename (value int) LOCATION '${hiveconf:test.blobstore.path.unique}/parallel_directory_rename/';
INSERT INTO parallel_directory_rename VALUES (1), (10), (100), (1000);
SELECT * FROM parallel_directory_rename;
