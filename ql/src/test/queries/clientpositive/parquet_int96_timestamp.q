create table dummy (id int);
insert into table dummy values (1);

set hive.parquet.mr.int96.enable.utc.write.zone=true;
set hive.parquet.timestamp.skip.conversion=true;

-- read/write timestamps using UTC as default write zone
create table timestamps (ts timestamp) stored as parquet;
insert into table timestamps select cast('2016-01-01 01:01:01' as timestamp) limit 1;
select * from timestamps;
describe formatted timestamps;
drop table timestamps;

-- table property is set. the default should not override it
create table timestamps (ts timestamp) stored as parquet tblproperties('parquet.mr.int96.write.zone'='PST');
insert into table timestamps select cast('2016-01-01 01:01:01' as timestamp) limit 1;
select * from timestamps;
describe formatted timestamps;
drop table timestamps;

set hive.parquet.mr.int96.enable.utc.write.zone=false;

-- read/write timestamps using local timezone
create table timestamps (ts timestamp) stored as parquet;
insert into table timestamps select cast('2016-01-01 01:01:01' as timestamp) limit 1;
select * from timestamps;
describe formatted timestamps;
drop table timestamps;

-- read/write timestamps with timezone specified in table properties
create table timestamps (ts timestamp) stored as parquet tblproperties('parquet.mr.int96.write.zone'='CST');
insert into table timestamps select cast('2016-01-01 01:01:01' as timestamp) limit 1;
select * from timestamps;
describe formatted timestamps;
drop table timestamps;

-- read/write timestamps with timezone specified in table properties
create table timestamps (ts timestamp) stored as parquet tblproperties('parquet.mr.int96.write.zone'='PST');
insert into table timestamps select cast('2016-01-01 01:01:01' as timestamp) limit 1;
insert into table timestamps values('2017-01-01 01:01:01');
-- parquet timezone flag set in the fetch operator
select * from timestamps;
-- parquet timezone flag set in MapredParquetInputFormat
select * from timestamps order by ts;
select * from timestamps where ts = cast('2016-01-01 01:01:01' as timestamp);
-- using udfs
select year(ts), day(ts), hour(ts), ts from timestamps;
describe formatted timestamps;
drop table timestamps;

-- read timestamps with different timezones specified in two table properties
create table timestamps (ts timestamp) stored as parquet tblproperties('parquet.mr.int96.write.zone'='PST');
insert into table timestamps select cast('2016-01-01 01:01:01' as timestamp) limit 1;
insert into table timestamps values('2017-01-01 01:01:01');
create table timestamps2 (ts timestamp) stored as parquet tblproperties('parquet.mr.int96.write.zone'='GMT+2');
insert into table timestamps2 select cast('2016-01-01 01:01:01' as timestamp) limit 1;
insert into table timestamps2 values('2017-01-01 01:01:01');
-- parquet timezone flag set in the MapredLocalTask
select * from timestamps a inner join timestamps2 b on a.ts = b.ts;
describe formatted timestamps;
drop table timestamps;
describe formatted timestamps2;
drop table timestamps2;

-- read timestamps written by Impala
create table timestamps (ts timestamp) stored as parquet;
load data local inpath '../../data/files/impala_int96_timestamp.parq' overwrite into table timestamps;
select * from timestamps;
drop table timestamps;

-- read timestamps written by Impala when table timezone is set (Impala timestamp should not be converted)
create table timestamps (ts timestamp) stored as parquet tblproperties('parquet.mr.int96.write.zone'='GMT+10');
load data local inpath '../../data/files/impala_int96_timestamp.parq' overwrite into table timestamps;
select * from timestamps;
drop table timestamps;

-- CREATE TABLE LIKE <OTHER TABLE> will copy the timezone property
create table timestamps (ts timestamp) stored as parquet tblproperties('parquet.mr.int96.write.zone'='GMT+10');
create table timestamps2 like timestamps;
describe formatted timestamps;
describe formatted timestamps2;
drop table timestamps;
drop table timestamps2;

drop table if exists dummy;