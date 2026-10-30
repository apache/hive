SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

create table tsstat (ts timestamp) stored as orc;
insert into tsstat values ("1970-01-01 00:00:00.00005");

SET hive.vectorized.execution.enabled=false;
select * from tsstat where ts = "1970-01-01 00:00:00.00005";
select * from tsstat where ts = "1970-01-01 00:00:00.000050";
select * from tsstat where ts = "1970-01-01 00:00:00.000050000";
select * from tsstat where ts = "1970-01-01 00:00:00.000050001";

select * from tsstat where ts = "1970-01-01 00:00:00.00005";
select * from tsstat where ts = "1970-01-01 00:00:00.000050";
select * from tsstat where ts = "1970-01-01 00:00:00.000050000";
select * from tsstat where ts = "1970-01-01 00:00:00.000050001";

SET hive.vectorized.execution.enabled=true;
select * from tsstat where ts = "1970-01-01 00:00:00.00005";
select * from tsstat where ts = "1970-01-01 00:00:00.000050";
select * from tsstat where ts = "1970-01-01 00:00:00.000050000";
select * from tsstat where ts = "1970-01-01 00:00:00.000050001";

select * from tsstat where ts = "1970-01-01 00:00:00.00005";
select * from tsstat where ts = "1970-01-01 00:00:00.000050";
select * from tsstat where ts = "1970-01-01 00:00:00.000050000";
select * from tsstat where ts = "1970-01-01 00:00:00.000050001";
