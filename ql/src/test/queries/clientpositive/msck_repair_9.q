DROP TABLE IF EXISTS tbl_x;
DROP TABLE IF EXISTS tbl_y;

CREATE EXTERNAL TABLE tbl_x (id INT, name STRING) PARTITIONED BY (month INT, day INT) stored as ORC location '${system:test.tmp.dir}/apps/hive/warehouse/test.db/tbl_x/';

INSERT INTO tbl_x values(1, 'aaa', 12, 2);
INSERT INTO tbl_x values(2, 'bbb', 12, 3);
INSERT INTO tbl_x (id, name, month) values(3, 'ccc', 12);

alter table tbl_x set default partition to 'ANOTHER_PARTITION';
INSERT INTO tbl_x (id, name, day) values(4, 'ddd', 3);

SHOW PARTITIONS tbl_x;

CREATE EXTERNAL TABLE tbl_y (id INT, name STRING) PARTITIONED BY (month INT, day INT) stored as ORC location '${system:test.tmp.dir}/apps/hive/warehouse/test.db/tbl_x/';
alter table tbl_y set default partition to 'ANOTHER_PARTITION';

set hive.msck.path.validation=skip;

MSCK REPAIR TABLE tbl_y;

SHOW PARTITIONS tbl_y;

alter table tbl_y set default partition to 'SECOND_PARTITION';
INSERT INTO tbl_y (id, name, day) values(4, 'ddd', 3);
SHOW PARTITIONS tbl_y;

alter table tbl_y set default partition to 'OTHER_PARTITION';
INSERT INTO tbl_y (id, name, day) values(4, 'ddd', 3);
SHOW PARTITIONS tbl_y;

set hive.msck.path.validation=ignore;

dfs ${system:test.dfs.mkdir} -p ${system:test.tmp.dir}/apps/hive/warehouse/test.db//tbl_x/month=DEFAULT/day=DEFAULT;
MSCK REPAIR TABLE tbl_y;
SHOW PARTITIONS tbl_y;

DROP TABLE tbl_x;
DROP TABLE tbl_y;
