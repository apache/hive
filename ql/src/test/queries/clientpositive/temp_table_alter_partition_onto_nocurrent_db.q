CREATE DATABASE test_db_nocurr;

CREATE TEMPORARY TABLE test_db_nocurr.test_table_for_alter_partition_nocurrentdb_temp (a INT) PARTITIONED BY (ds STRING) STORED AS SEQUENCEFILE;

ALTER TABLE test_db_nocurr.test_table_for_alter_partition_nocurrentdb_temp Add PARTITION(ds='eleme_haihua');

INSERT OVERWRITE TABLE test_db_nocurr.test_table_for_alter_partition_nocurrentdb_temp PARTITION(ds='eleme_haihua') SELECT 1;

desc extended test_db_nocurr.test_table_for_alter_partition_nocurrentdb_temp partition(ds='eleme_haihua');

ALTER TABLE test_db_nocurr.test_table_for_alter_partition_nocurrentdb_temp partition(ds='eleme_haihua') CHANGE COLUMN a a_new BOOLEAN;

desc extended test_db_nocurr.test_table_for_alter_partition_nocurrentdb_temp partition(ds='eleme_haihua');

DROP TABLE test_db_nocurr.test_table_for_alter_partition_nocurrentdb_temp;

DROP DATABASE test_db_nocurr;
