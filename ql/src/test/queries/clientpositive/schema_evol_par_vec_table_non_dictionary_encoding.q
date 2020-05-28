--! qt:disabled:HIVE-23566
set hive.fetch.task.conversion=none;
set hive.vectorized.execution.enabled=true;
set parquet.enable.dictionary=false;

-- SORT_QUERY_RESULTS

drop table test_alter_n0;
drop table test_alter2_n0;
drop table test_alter3_n0;

create table test_alter_n0 (id string) stored as parquet;
insert into test_alter_n0 values ('1'), ('2'), ('3');
select * from test_alter_n0;

-- add new column -> empty col values should return NULL
alter table test_alter_n0 add columns (newCol string);
select * from test_alter_n0;

-- insert data into new column -> New data should be returned
insert into test_alter_n0 values ('4', '100');
select * from test_alter_n0;

-- remove the newly added column
-- this works in vectorized execution
alter table test_alter_n0 replace columns (id string);
select * from test_alter_n0;

-- add column using replace column syntax
alter table test_alter_n0 replace columns (id string, id2 string);
-- this surprisingly doesn't return the 100 added to 4th row above
select * from test_alter_n0;
insert into test_alter_n0 values ('5', '100');
select * from test_alter_n0;

-- use the same column name and datatype
alter table test_alter_n0 replace columns (id string, id2 string);
select * from test_alter_n0;

-- change string to char
alter table test_alter_n0 replace columns (id char(10), id2 string);
select * from test_alter_n0;

-- change string to varchar
alter table test_alter_n0 replace columns (id string, id2 string);
alter table test_alter_n0 replace columns (id varchar(10), id2 string);
select * from test_alter_n0;

-- change columntype and column name
alter table test_alter_n0 replace columns (id string, id2 string);
alter table test_alter_n0 replace columns (idv varchar(10), id2 string);
select * from test_alter_n0;

-- test int to long type conversion
create table test_alter2_n0 (id int) stored as parquet;
insert into test_alter2_n0 values (1);
alter table test_alter2_n0 replace columns (id bigint);
select * from test_alter2_n0;

-- test float to double type conversion
drop table test_alter2_n0;
create table test_alter2_n0 (id float) stored as parquet;
insert into test_alter2_n0 values (1.5);
alter table test_alter2_n0 replace columns (id double);
select * from test_alter2_n0;

drop table test_alter2_n0;
create table test_alter2_n0 (ts timestamp) stored as parquet;
insert into test_alter2_n0 values ('2018-01-01 13:14:15.123456'), ('2018-01-02 14:15:16.123456'), ('2018-01-03 16:17:18.123456');
select * from test_alter2_n0;
alter table test_alter2_n0 replace columns (ts string);
select * from test_alter2_n0;

drop table test_alter2_n0;
create table test_alter2_n0 (ts timestamp) stored as parquet;
insert into test_alter2_n0 values ('2018-01-01 13:14:15.123456'), ('2018-01-02 14:15:16.123456'), ('2018-01-03 16:17:18.123456');
select * from test_alter2_n0;
alter table test_alter2_n0 replace columns (ts varchar(19));
-- this should truncate the microseconds
select * from test_alter2_n0;

drop table test_alter2_n0;
create table test_alter2_n0 (ts timestamp) stored as parquet;
insert into test_alter2_n0 values ('2018-01-01 13:14:15.123456'), ('2018-01-02 14:15:16.123456'), ('2018-01-03 16:17:18.123456');
select * from test_alter2_n0;
alter table test_alter2_n0 replace columns (ts char(25));
select * from test_alter2_n0;

-- test integer types upconversion
create table test_alter3_n0 (id1 tinyint, id2 smallint, id3 int, id4 bigint) stored as parquet;
insert into test_alter3_n0 values (10, 20, 30, 40);
alter table test_alter3_n0 replace columns (id1 smallint, id2 int, id3 bigint, id4 decimal(10,4));
-- this fails mostly due to bigint to decimal
-- select * from test_alter3_n0;
select id1, id2, id3 from test_alter3_n0;


