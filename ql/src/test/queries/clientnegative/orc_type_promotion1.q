SET hive.exec.schema.evolution=true;

-- Currently, string to int conversion is not supported because it isn't in the lossless
-- TypeIntoUtils.implicitConvertible conversions.
create table src_orc (key string, val string) stored as orc;
alter table src_orc change key key int;
