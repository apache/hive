set hive.mapred.mode=nonstrict;
set hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;

DROP TABLE orc_struct_type_staging;
DROP TABLE orc_struct_type;

CREATE TABLE orc_struct_type_staging (
id int,
st1 struct<f1:int, f2:string>,
st2 struct<f1:int, f3:string>
) ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '|'
  COLLECTION ITEMS TERMINATED BY ',';

CREATE TABLE orc_struct_type (
id int,
st1 struct<f1:int, f2:string>,
st2 struct<f1:int, f3:string>
) STORED AS ORC;

LOAD DATA LOCAL INPATH '../../data/files/struct_type.txt' OVERWRITE INTO TABLE orc_struct_type_staging;
-- test data size < 1024
INSERT OVERWRITE TABLE orc_struct_type
SELECT id, st1, st2 FROM orc_struct_type_staging where id < 1024;

-- verify the row number
select count(*) from orc_struct_type;
-- test select all columns and fields
explain vectorization expression select st1, st1.f1, st1.f2, st2, st2.f1, st2.f3 from orc_struct_type limit 10;
select st1, st1.f1, st1.f2, st2, st2.f1, st2.f3 from orc_struct_type limit 10;
-- test select fields only
select st1.f1, st2.f1, st2.f3 from orc_struct_type limit 10;
select st1.f1, st2.f1 from orc_struct_type limit 10;
-- test complex select with list
explain vectorization expression select sum(st1.f1), st1.f1 from orc_struct_type where st1.f1 > 500 group by st1.f1 limit 10;
select sum(st1.f1), st1.f1 from orc_struct_type where st1.f1 > 500 group by st1.f1 order by st1.f1 limit 10;

-- test data size = 1024
INSERT OVERWRITE TABLE orc_struct_type
SELECT id, st1, st2 FROM orc_struct_type_staging where id < 1025;

-- verify the row number
select count(*) from orc_struct_type;
-- test select all columns and fields
select st1, st1.f1, st1.f2, st2, st2.f1, st2.f3 from orc_struct_type limit 10;
-- test select fields only
select st1.f1, st2.f1, st2.f3 from orc_struct_type limit 10;
select st1.f1, st2.f1 from orc_struct_type limit 10;
-- test complex select with list
select sum(st1.f1), st1.f1 from orc_struct_type where st1.f1 > 500 group by st1.f1 order by st1.f1 limit 10;

-- test data size = 1025
INSERT OVERWRITE TABLE orc_struct_type
SELECT id, st1, st2 FROM orc_struct_type_staging where id < 1026;

-- verify the row number
select count(*) from orc_struct_type;
-- test select all columns and fields
select st1, st1.f1, st1.f2, st2, st2.f1, st2.f3 from orc_struct_type limit 10;
-- test select fields only
select st1.f1, st2.f1, st2.f3 from orc_struct_type limit 10;
select st1.f1, st2.f1 from orc_struct_type limit 10;
-- test complex select with list
select sum(st1.f1), st1.f1 from orc_struct_type where st1.f1 > 500 group by st1.f1 order by st1.f1 limit 10;