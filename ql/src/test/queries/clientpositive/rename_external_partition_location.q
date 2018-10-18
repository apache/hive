--! qt:dataset:src
--! qt:dataset:part
set hive.mapred.mode=nonstrict;

dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/ex_table;

CREATE EXTERNAL TABLE ex_table ( key INT, value STRING)
    PARTITIONED BY (part STRING)
    STORED AS textfile
	LOCATION 'file:${system:test.tmp.dir}/ex_table';

INSERT OVERWRITE TABLE ex_table PARTITION (part='part1')
SELECT key, value FROM src WHERE key < 10;

SHOW PARTITIONS ex_table;
SELECT * from ex_table where part='part1' ORDER BY key;

dfs -ls ${system:test.tmp.dir}/ex_table/part=part1;
dfs -cat ${system:test.tmp.dir}/ex_table/part=part1/000000_0;

ANALYZE TABLE ex_table PARTITION (part='part1') COMPUTE STATISTICS FOR COLUMNS;
DESCRIBE FORMATTED ex_table;
DESCRIBE FORMATTED ex_table PARTITION (part='part1');
DESCRIBE FORMATTED ex_table PARTITION (part='part1') key;
DESCRIBE FORMATTED ex_table PARTITION (part='part1') value;

ALTER TABLE ex_table PARTITION (part='part1') RENAME TO PARTITION (part='part2');

SHOW PARTITIONS ex_table;
SELECT * from ex_table where part='part2' ORDER BY key;

dfs -ls ${system:test.tmp.dir}/ex_table/part=part1;
dfs -cat ${system:test.tmp.dir}/ex_table/part=part1/000000_0;

DESCRIBE FORMATTED ex_table;
DESCRIBE FORMATTED ex_table PARTITION (part='part2');
DESCRIBE FORMATTED ex_table PARTITION (part='part2') key;
DESCRIBE FORMATTED ex_table PARTITION (part='part2') value;
