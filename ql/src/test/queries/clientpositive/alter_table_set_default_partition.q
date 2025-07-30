drop table if exists tbl_orc;

create external table tbl_orc(a int, b string, c int) partitioned by (d int) stored as orc;
describe formatted tbl_orc;
INSERT INTO tbl_orc (a, b, c) values(3, 'ccc', 12);
show partitions tbl_orc;
dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/tbl_orc/;
alter table tbl_orc set default partition name to 'random_partition';
describe formatted tbl_orc;
show partitions tbl_orc;
select * from tbl_orc;
INSERT INTO tbl_orc (a, b, c) values(4, 'ddd', 23);
show partitions tbl_orc;
select * from tbl_orc;
dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/tbl_orc/;
drop table if exists tbl_orc;


create external table tbl_orc(a int, b string, c int) partitioned by (d int, e string, f bigint) stored as orc;
describe formatted tbl_orc;
INSERT INTO tbl_orc (a, b, c) values(3, 'ccc', 12);
INSERT INTO tbl_orc (a, b, c, d, e) values(5, 'ddd', 13, 32, 'tgf');
INSERT INTO tbl_orc (a, b, c, e, f) values(6, 'eee', 14, 'tgh', 23456876);
show partitions tbl_orc;
dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/tbl_orc/;
alter table tbl_orc set default partition name to 'default_partition';
describe formatted tbl_orc;
select * from tbl_orc;
show partitions tbl_orc;
INSERT INTO tbl_orc (a, b, c) values(4, 'ddd', 23);
select * from tbl_orc;
show partitions tbl_orc;
dfs -ls ${hiveconf:hive.metastore.warehouse.dir}/tbl_orc/;
drop table if exists tbl_orc;