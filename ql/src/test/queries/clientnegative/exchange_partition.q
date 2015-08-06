dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/ex_table1;
dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/ex_table1/part=part1;
CREATE EXTERNAL TABLE ex_table1 ( key INT, value STRING)
    PARTITIONED BY (part STRING)
    STORED AS textfile
        LOCATION 'file:${system:test.tmp.dir}/ex_table1';

dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/ex_table2;

CREATE EXTERNAL TABLE ex_table2 ( key INT, value STRING)
    PARTITIONED BY (part STRING)
    STORED AS textfile
	LOCATION 'file:${system:test.tmp.dir}/ex_table2';

INSERT OVERWRITE TABLE ex_table2 PARTITION (part='part1')
SELECT key, value FROM src WHERE key < 10;
SHOW PARTITIONS ex_table2;

ALTER TABLE ex_table1 EXCHANGE PARTITION (part='part1') WITH TABLE ex_table2;
