SET hive.exec.schema.evolution=false;

-- Currently, bigint to int conversion is not supported because it isn't in the lossless
-- TypeIntoUtils.implicitConvertible conversions.
create table src_orc (key smallint, val string) clustered by (val) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');
desc src_orc;
alter table src_orc change key key smallint;
desc src_orc;
alter table src_orc change key key int;
desc src_orc;
alter table src_orc change key key bigint;
desc src_orc;
alter table src_orc change val val int;
