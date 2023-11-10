-- Mask the totalSize value as it can have slight variability, causing test flakiness
--! qt:replace:/(\s+totalSize\s+)\S+(\s+)/$1#Masked#$2/
-- Mask random uuid
--! qt:replace:/(\s+uuid\s+)\S+(\s*)/$1#Masked#$2/
-- SORT_QUERY_RESULTS

set hive.vectorized.execution.enabled=false;

-- Create a hive and iceberg table to compare.
create table hiveT1 (a string, b int, c int) PARTITIONED BY (d_part int, e_part int) stored as orc ;
insert into hiveT1 values ('aa', 1, 2, 3, 4), ('aa', 1, 2, 3, 4), ('aa', 1, 2, 2, 5), ('aa', 1, 2, 10, 5), ('aa', 1, 2, 10, 5);
create table ice1 (a string, b int, c int) PARTITIONED BY (d_part int, e_part int) stored by iceberg stored as orc TBLPROPERTIES("format-version"='2') ;
insert into ice1 values  ('aa', 1, 2, 3, 4), ('aa', 1, 2, 3, 4), ('aa', 1, 2, 2, 5), ('aa', 1, 2, 10, 5), ('aa', 1, 2, 10, 5);

--compare hive table  with iceberg table
show partitions hiveT1;
describe default.ice1.partitions;
select `partition` from default.ice1.partitions order by `partition`;
show partitions ice1 ;


explain show partitions hiveT1;
explain show partitions ice1;
explain select * from default.ice1.partitions;

---- Partition evolution
create table ice2 (a string, b int, c int) PARTITIONED BY (d_part int, e_part int) stored by iceberg stored as orc
TBLPROPERTIES("format-version"='2') ;
insert into ice2 values  ('aa', 1, 2, 3, 4), ('aa', 1, 2, 3, 4), ('aa', 1, 2, 2, 5), ('aa', 1, 2, 10, 5), ('aa', 1, 2,10, 5);

select `partition` from default.ice2.partitions order by `partition`;
show partitions ice2;

ALTER TABLE ice2 SET PARTITION SPEC (c) ;
select `partition` from default.ice2.partitions order by `partition`;
show partitions ice2;

insert into ice2 values  ('aa', 1, 2, 3, 4), ('aa', 1, 2, 3, 4), ('aa', 1, 3, 2, 5), ('aa', 1, 4, 10, 5), ('aa', 1, 5,
10, 5);
select `partition` from default.ice2.partitions order by `partition`;
show partitions ice2;


