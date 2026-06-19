CREATE DATABASE test_db_nocurr;

CREATE TABLE test_db_nocurr.test_table_for_alter_partition_nocurrentdb (a INT) PARTITIONED BY (ds STRING) STORED AS SEQUENCEFILE;

INSERT OVERWRITE TABLE test_db_nocurr.test_table_for_alter_partition_nocurrentdb PARTITION(ds='eleme_haihua') SELECT 1;

desc extended test_db_nocurr.test_table_for_alter_partition_nocurrentdb partition(ds='eleme_haihua');

ALTER TABLE test_db_nocurr.test_table_for_alter_partition_nocurrentdb partition(ds='eleme_haihua') CHANGE COLUMN a a_new BOOLEAN;

desc extended test_db_nocurr.test_table_for_alter_partition_nocurrentdb partition(ds='eleme_haihua');

DROP TABLE test_db_nocurr.test_table_for_alter_partition_nocurrentdb;

DROP DATABASE test_db_nocurr;
