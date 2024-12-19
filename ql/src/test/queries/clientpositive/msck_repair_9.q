DROP TABLE IF EXISTS tbl_x;
DROP TABLE IF EXISTS tbl_y;

CREATE EXTERNAL TABLE tbl_x (id INT, name STRING) PARTITIONED BY (month INT, day INT) stored as ORC location '${system:test.tmp.dir}/apps/hive/warehouse/test.db/tbl_x/';

INSERT INTO tbl_x values(1, 'aaa', 12, 2);
INSERT INTO tbl_x values(2, 'bbb', 12, 3);
INSERT INTO tbl_x (id, name, month) values(3, 'ccc', 12);

SET hive.exec.default.partition.name=ANOTHER_PARTITION;
INSERT INTO tbl_x (id, name, day) values(4, 'ddd', 3);

SHOW PARTITIONS tbl_x;

CREATE EXTERNAL TABLE tbl_y (id INT, name STRING) PARTITIONED BY (month INT, day INT) stored as ORC location '${system:test.tmp.dir}/apps/hive/warehouse/test.db/tbl_x/';

MSCK REPAIR TABLE tbl_y;

SHOW PARTITIONS tbl_y;

DROP TABLE tbl_x;
DROP TABLE tbl_y;
