SET hive.exec.schema.evolution=true;

-- Currently, smallint to tinyint conversion is not supported because it isn't in the lossless
-- TypeIntoUtils.implicitConvertible conversions.
create table src_orc (key smallint, val string) stored as orc;
alter table src_orc replace columns (k int, val string, z smallint);
alter table src_orc replace columns (k int, val string, z tinyint);
