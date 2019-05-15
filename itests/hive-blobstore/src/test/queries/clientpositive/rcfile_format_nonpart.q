-- Test INSERT OVERWRITE and INSERT INTO on rcfile table in blobstore

DROP TABLE blobstore_source;
CREATE TABLE blobstore_source(a STRING, b STRING, c DOUBLE)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ' '
COLLECTION ITEMS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
LOCATION '${hiveconf:test.blobstore.path.unique}/rcfile_format_nonpart/blobstore_source/';
LOAD DATA LOCAL INPATH '../../data/files/3col_data.txt' INTO TABLE blobstore_source;

DROP TABLE rcfile_table;
CREATE TABLE rcfile_table (a INT, b STRING, value DOUBLE) STORED AS RCFILE
LOCATION '${hiveconf:test.blobstore.path.unique}/rcfile_format_nonpart/rcfile_table';
 
INSERT OVERWRITE TABLE rcfile_table
SELECT * FROM blobstore_source;
 
SELECT * FROM rcfile_table;
SELECT a FROM rcfile_table GROUP BY a;
SELECT b FROM rcfile_table GROUP BY b;
SELECT VALUE FROM rcfile_table GROUP BY VALUE;

INSERT INTO TABLE rcfile_table
SELECT * FROM blobstore_source;

SELECT * FROM rcfile_table;
SELECT a FROM rcfile_table GROUP BY a;
SELECT b FROM rcfile_table GROUP BY b;
SELECT value FROM rcfile_table GROUP BY value;