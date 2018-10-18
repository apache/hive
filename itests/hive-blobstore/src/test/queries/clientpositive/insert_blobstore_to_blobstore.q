-- Test inserting into a blobstore table from another blobstore table.

DROP TABLE blobstore_source;
CREATE TABLE blobstore_source (
    a string,
    b string,
    c double)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ' '
COLLECTION ITEMS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
LOCATION '${hiveconf:test.blobstore.path.unique}/insert_blobstore_to_blobstore/blobstore_source';

LOAD DATA LOCAL INPATH '../../data/files/3col_data.txt' INTO TABLE blobstore_source;

DROP TABLE blobstore_table;
CREATE TABLE blobstore_table LIKE blobstore_source
LOCATION '${hiveconf:test.blobstore.path.unique}/insert_blobstore_to_blobstore/blobstore_table';

INSERT OVERWRITE TABLE blobstore_table SELECT * FROM blobstore_source;

SELECT COUNT(*) FROM blobstore_table;

-- INSERT INTO should append all records to existing ones.
INSERT INTO TABLE blobstore_table SELECT * FROM blobstore_source;

SELECT COUNT(*) FROM blobstore_table;

SELECT * FROM blobstore_table;
