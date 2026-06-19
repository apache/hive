SET hive.exec.schema.evolution=true;

-- Currently, bigint to int conversion is not supported because it isn't in the lossless
-- TypeIntoUtils.implicitConvertible conversions.
create table src_orc (key smallint, val string) stored as orc;
desc src_orc;
alter table src_orc change key key smallint;
desc src_orc;
alter table src_orc change key key int;
desc src_orc;
alter table src_orc change key key bigint;
desc src_orc;
alter table src_orc change val val int;
