set hive.exec.dynamic.partition.mode=nonstrict;
set hive.load.dynamic.partitions.scan.specific.partitions=true;

CREATE EXTERNAL TABLE test_part_ext_1(i int) PARTITIONED BY (j string, k int);

CREATE TABLE test_part(i int,j string);

insert into test_part values(1, "test1"), (2, "test2"), (3, "test3"), 
(4, "test4"), (5, "test5"), (6, "test6"), (7, "test7"), (8, "test8"), (9, "test9"), (10, "test10");

insert overwrite table test_part_ext_1 partition(j, k) select i,j,i from test_part;

insert overwrite table test_part_ext_1 partition(j, k) select i,j,i from test_part;
