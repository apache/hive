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
select * from timestamps;
describe formatted timestamps;
drop table timestamps;

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