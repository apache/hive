SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

create table tsstat (ts timestamp) stored as orc;
insert into tsstat values ("1970-01-01 00:00:00.0005");

SET hive.vectorized.execution.enabled=false;
set hive.optimize.index.filter=false;
select * from tsstat where ts = "1970-01-01 00:00:00.0005";

set hive.optimize.index.filter=true;
select * from tsstat where ts = "1970-01-01 00:00:00.0005";

SET hive.vectorized.execution.enabled=true;
set hive.optimize.index.filter=false;
select * from tsstat where ts = "1970-01-01 00:00:00.0005";

set hive.optimize.index.filter=true;
select * from tsstat where ts = "1970-01-01 00:00:00.0005";