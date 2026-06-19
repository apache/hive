--! qt:dataset:src
--! qt:dataset:part
-- This test verifies that if the table after rename can still fetch the column statistics
set hive.stats.kll.enable=true;
set metastore.stats.fetch.bitvector=true;
set metastore.stats.fetch.kll=true;
set hive.stats.autogather=true;
set hive.stats.column.autogather=true;

CREATE TABLE rename_partition_table0 (key STRING, value STRING) PARTITIONED BY (part STRING)
STORED AS ORC;

INSERT OVERWRITE TABLE rename_partition_table0 PARTITION (part = '1') SELECT * FROM src where rand(1) < 0.5;
ALTER TABLE rename_partition_table0 ADD COLUMNS (new_col INT);
INSERT OVERWRITE TABLE rename_partition_table0 PARTITION (part = '2') SELECT src.*, 1 FROM src;

ALTER TABLE rename_partition_table0 RENAME TO rename_partition_table1;
DESCRIBE FORMATTED rename_partition_table1;
DESCRIBE FORMATTED rename_partition_table1 PARTITION (part='1') key;
DESCRIBE FORMATTED rename_partition_table1 PARTITION (part='1') value;
DESCRIBE FORMATTED rename_partition_table1 PARTITION (part='2') key;
DESCRIBE FORMATTED rename_partition_table1 PARTITION (part='2') value;
DESCRIBE FORMATTED rename_partition_table1 PARTITION (part='2') new_col;

CREATE EXTERNAL TABLE rename_partition_table_ext0 (key STRING, value STRING) PARTITIONED BY (part STRING)
STORED AS ORC;

INSERT OVERWRITE TABLE rename_partition_table_ext0 PARTITION (part = '1') SELECT * FROM src where rand(1) < 0.5;
ALTER TABLE rename_partition_table_ext0 CHANGE COLUMN value val STRING CASCADE;
INSERT OVERWRITE TABLE rename_partition_table_ext0 PARTITION (part = '2') SELECT * FROM src;

ALTER TABLE rename_partition_table_ext0 RENAME TO rename_partition_table_ext1;
DESCRIBE FORMATTED rename_partition_table_ext1;
DESCRIBE FORMATTED rename_partition_table_ext1 PARTITION (part='1') key;
DESCRIBE FORMATTED rename_partition_table_ext1 PARTITION (part='1') val;
DESCRIBE FORMATTED rename_partition_table_ext1 PARTITION (part='2') key;
DESCRIBE FORMATTED rename_partition_table_ext1 PARTITION (part='2') val;

DROP TABLE rename_partition_table1;
DROP TABLE rename_partition_table_ext1;
