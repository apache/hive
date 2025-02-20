-- Mask random uuid
--! qt:replace:/(\s+uuid\s+)\S+(\s*)/$1#Masked#$2/
-- SORT_QUERY_RESULTS

set hive.vectorized.execution.enabled=false;

-- Create a hive and iceberg table to compare.
create table hiveT1 (a string, b int, c int) PARTITIONED BY (d_part int, e_part int) stored as orc ;
create table ice1 (a string, b int, c int) PARTITIONED BY (d_part int, e_part int) stored by iceberg stored as orc TBLPROPERTIES("format-version"='2') ;

-- Compare empty hive and iceberg tables
show partitions hiveT1;
describe default.ice1.partitions;
select * from default.ice1.partitions;
show partitions ice1;

-- Insert data
insert into hiveT1 values ('aa', 1, 2, 3, 4), ('aa', 1, 2, 3, 4), ('aa', 1, 2, 2, 5), ('aa', 1, 2, 10, 5), ('aa', 1, 2, 10, 5);
insert into ice1 values  ('aa', 1, 2, 3, 4), ('aa', 1, 2, 3, 4), ('aa', 1, 2, 2, 5), ('aa', 1, 2, 10, 5), ('aa', 1, 2, 10, 5);

-- Compare hive and iceberg tables
show partitions hiveT1;
select `partition` from default.ice1.partitions;
show partitions ice1;

explain show partitions hiveT1;
explain show partitions ice1;
explain select * from default.ice1.partitions;

---- Partition evolution
create table ice2 (a string, b int, c int) PARTITIONED BY (d_part int, e_part int) stored by iceberg stored as orc
TBLPROPERTIES("format-version"='2') ;
insert into ice2 values  ('aa', 1, 2, 3, 4), ('aa', 1, 2, 3, 4), ('aa', 1, 2, 2, 5), ('aa', 1, 2, 10, 5), ('aa', 1, 2,10, 5);

select `partition` from default.ice2.partitions;
show partitions ice2;

ALTER TABLE ice2 SET PARTITION SPEC (c) ;
select `partition` from default.ice2.partitions;
show partitions ice2;

insert into ice2 values  ('aa', 1, 2, 3, 4), ('aa', 1, 2, 3, 4), ('aa', 1, 3, 2, 5), ('aa', 1, 4, 10, 5), ('aa', 1, 5,
10, 5);
select `partition` from default.ice2.partitions;
show partitions ice2;