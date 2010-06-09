set hive.archive.enabled = true;
set hive.enforce.bucketing = true;

-- EXCLUDE_HADOOP_MAJOR_VERSIONS(0.17, 0.18, 0.19)

SELECT SUM(hash(col)) FROM (SELECT transform(*) using 'tr "\t" "_"' AS col 
FROM (SELECT * FROM srcpart WHERE ds='2008-04-08') subq1) subq2;

ALTER TABLE srcpart ARCHIVE PARTITION (ds='2008-04-08', hr='12');

SELECT SUM(hash(col)) FROM (SELECT transform(*) using 'tr "\t" "_"' AS col 
FROM (SELECT * FROM srcpart WHERE ds='2008-04-08') subq1) subq2;

ALTER TABLE srcpart UNARCHIVE PARTITION (ds='2008-04-08', hr='12');

SELECT SUM(hash(col)) FROM (SELECT transform(*) using 'tr "\t" "_"' AS col 
FROM (SELECT * FROM srcpart WHERE ds='2008-04-08') subq1) subq2;

CREATE TABLE harbucket(key INT) 
PARTITIONED by (ds STRING)
CLUSTERED BY (key) INTO 10 BUCKETS;

INSERT OVERWRITE TABLE harbucket PARTITION(ds='1') SELECT CAST(key AS INT) AS a FROM src WHERE key < 50;

SELECT key FROM harbucket TABLESAMPLE(BUCKET 1 OUT OF 10) SORT BY key;
ALTER TABLE srcpart ARCHIVE PARTITION (ds='2008-04-08', hr='12');
SELECT key FROM harbucket TABLESAMPLE(BUCKET 1 OUT OF 10) SORT BY key;
ALTER TABLE srcpart UNARCHIVE PARTITION (ds='2008-04-08', hr='12');
SELECT key FROM harbucket TABLESAMPLE(BUCKET 1 OUT OF 10) SORT BY key;

DROP TABLE harbucket;

CREATE TABLE old_name(key INT) 
PARTITIONED by (ds STRING);

INSERT OVERWRITE TABLE old_name PARTITION(ds='1') SELECT CAST(key AS INT) AS a FROM src WHERE key < 50;
ALTER TABLE old_name ARCHIVE PARTITION (ds='1');
SELECT SUM(hash(col)) FROM (SELECT transform(*) using 'tr "\t" "_"' AS col 
FROM (SELECT * FROM old_name WHERE ds='1') subq1) subq2;
ALTER TABLE old_name RENAME TO new_name;
SELECT SUM(hash(col)) FROM (SELECT transform(*) using 'tr "\t" "_"' AS col 
FROM (SELECT * FROM new_name WHERE ds='1') subq1) subq2;

DROP TABLE new_name;
