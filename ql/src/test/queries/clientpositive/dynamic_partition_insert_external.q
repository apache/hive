dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/HIVE-11662/somekey=somevalue;

CREATE TABLE t1 (c1 BIGINT, c2 STRING);

CREATE EXTERNAL TABLE t2 (c1 INT, c2 STRING)
PARTITIONED BY (p1 STRING)
LOCATION '${system:test.tmp.dir}/HIVE-11662/somekey=somevalue';

LOAD DATA LOCAL INPATH '../../data/files/dynamic_partition_insert.txt' INTO TABLE t1;
LOAD DATA LOCAL INPATH '../../data/files/dynamic_partition_insert.txt' INTO TABLE t1;
LOAD DATA LOCAL INPATH '../../data/files/dynamic_partition_insert.txt' INTO TABLE t1;
LOAD DATA LOCAL INPATH '../../data/files/dynamic_partition_insert.txt' INTO TABLE t1;
LOAD DATA LOCAL INPATH '../../data/files/dynamic_partition_insert.txt' INTO TABLE t1;

SET hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE t2 partition(p1) SELECT *,c1 AS p1 FROM t1 DISTRIBUTE BY p1;

SELECT * FROM t2;

DROP TABLE t1;
DROP TABLE t2;

dfs -rmr ${system:test.tmp.dir}/HIVE-11662