-- Test INSERT OVERWRITE and INSERT INTO on orc table in blobstore

DROP TABLE blobstore_source;
CREATE TABLE blobstore_source(a STRING, b STRING, c DOUBLE)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ' '
COLLECTION ITEMS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
LOCATION '${hiveconf:test.blobstore.path.unique}/orc_format_nonpart/blobstore_source/';
LOAD DATA LOCAL INPATH '../../data/files/3col_data.txt' INTO TABLE blobstore_source;

DROP TABLE orc_table;
CREATE EXTERNAL TABLE orc_table (a INT, b STRING, value DOUBLE) STORED AS ORC
LOCATION '${hiveconf:test.blobstore.path.unique}/orc_format_nonpart/orc_table';
 
INSERT OVERWRITE TABLE orc_table
SELECT * FROM blobstore_source;
 
SELECT * FROM orc_table;
SELECT a FROM orc_table GROUP BY a;
SELECT b FROM orc_table GROUP BY b;
SELECT value FROM orc_table GROUP BY value;

INSERT INTO TABLE orc_table
SELECT * FROM blobstore_source;

SELECT * FROM orc_table;
SELECT a FROM orc_table GROUP BY a;
SELECT b FROM orc_table GROUP BY b;
SELECT value FROM orc_table GROUP BY value;