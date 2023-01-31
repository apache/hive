create table test_parquet_1 (id decimal) stored as parquet;
insert into test_parquet_1 values (238), (238.12), (-0.123);
alter table test_parquet_1 change id id string;
select * from test_parquet_1;

create table test_parquet_2 (id decimal(10,2)) stored as parquet;
insert into test_parquet_2 values (238), (238.12), (-0.123);
alter table test_parquet_2 change id id string;
select * from test_parquet_2;

create table test_parquet_3 (id decimal(4,2)) stored as parquet;
insert into test_parquet_3 values (238), (238.12), (-0.123);
alter table test_parquet_3 change id id string;
select * from test_parquet_3;

create table test_parquet_4 (id decimal(11,5)) stored as parquet;
insert into test_parquet_4 values (238), (238.12), (-0.123456789);
alter table test_parquet_4 change id id string;
select * from test_parquet_4;

create table test_parquet_5 (id decimal(10,2)) stored as parquet;
insert into test_parquet_5 values (238), (238.12), (-0.123);
alter table test_parquet_5 change id id string;
select * from test_parquet_5;

create table test_parquet_6 (id decimal(38,18)) stored as parquet;
insert into test_parquet_6 values (-12345678901234567890.098765432109876543), (12345678901234567890.098765432109876543), (124567.678950), (0987678904789.567849028679576658);
alter table test_parquet_6 change id id string;
select * from test_parquet_6;

drop table test_parquet_1;
drop table test_parquet_2;
drop table test_parquet_3;
drop table test_parquet_4;
drop table test_parquet_5;
drop table test_parquet_6;

create table test_parquet_1 (id decimal) stored as parquet;
insert into test_parquet_1 values (238), (238.12), (-0.123);
alter table test_parquet_1 change id id varchar(255);
select * from test_parquet_1;

create table test_parquet_2 (id decimal(10,2)) stored as parquet;
insert into test_parquet_2 values (238), (238.12), (-0.123);
alter table test_parquet_2 change id id varchar(255);
select * from test_parquet_2;

create table test_parquet_3 (id decimal(4,2)) stored as parquet;
insert into test_parquet_3 values (238), (238.12), (-0.123);
alter table test_parquet_3 change id id varchar(255);
select * from test_parquet_3;

create table test_parquet_4 (id decimal(11,5)) stored as parquet;
insert into test_parquet_4 values (238), (238.12), (-0.123456789);
alter table test_parquet_4 change id id varchar(255);
select * from test_parquet_4;

create table test_parquet_5 (id decimal(10,2)) stored as parquet;
insert into test_parquet_5 values (238), (238.12), (-0.123);
alter table test_parquet_5 change id id varchar(255);
select * from test_parquet_5;

create table test_parquet_6 (id decimal(38,18)) stored as parquet;
insert into test_parquet_6 values (-12345678901234567890.098765432109876543), (12345678901234567890.098765432109876543), (124567.678950), (0987678904789.567849028679576658);
alter table test_parquet_6 change id id varchar(255);
select * from test_parquet_6;

create table test_parquet_7 (id decimal(38,18)) stored as parquet;
insert into test_parquet_7 values (-12345678901234567890.098765432109876543), (12345678901234567890.098765432109876543), (124567.678950), (0987678904789.567849028679576658);
alter table test_parquet_7 change id id varchar(8);
select * from test_parquet_7;

drop table test_parquet_1;
drop table test_parquet_2;
drop table test_parquet_3;
drop table test_parquet_4;
drop table test_parquet_5;
drop table test_parquet_6;
drop table test_parquet_7;

create table test_parquet_1 (id decimal) stored as parquet;
insert into test_parquet_1 values (238), (238.12), (-0.123);
alter table test_parquet_1 change id id char(255);
select * from test_parquet_1;

create table test_parquet_2 (id decimal(10,2)) stored as parquet;
insert into test_parquet_2 values (238), (238.12), (-0.123);
alter table test_parquet_2 change id id char(255);
select * from test_parquet_2;

create table test_parquet_3 (id decimal(4,2)) stored as parquet;
insert into test_parquet_3 values (238), (238.12), (-0.123);
alter table test_parquet_3 change id id char(255);
select * from test_parquet_3;

create table test_parquet_4 (id decimal(11,5)) stored as parquet;
insert into test_parquet_4 values (238), (238.12), (-0.123456789);
alter table test_parquet_4 change id id char(255);
select * from test_parquet_4;

create table test_parquet_5 (id decimal(10,2)) stored as parquet;
insert into test_parquet_5 values (238), (238.12), (-0.123);
alter table test_parquet_5 change id id char(255);
select * from test_parquet_5;

create table test_parquet_6 (id decimal(38,18)) stored as parquet;
insert into test_parquet_6 values (-12345678901234567890.098765432109876543), (12345678901234567890.098765432109876543), (124567.678950), (0987678904789.567849028679576658);
alter table test_parquet_6 change id id char(255);
select * from test_parquet_6;

create table test_parquet_7 (id decimal(38,18)) stored as parquet;
insert into test_parquet_7 values (-12345678901234567890.098765432109876543), (12345678901234567890.098765432109876543), (124567.678950), (0987678904789.567849028679576658);
alter table test_parquet_7 change id id char(8);
select * from test_parquet_7;

drop table test_parquet_1;
drop table test_parquet_2;
drop table test_parquet_3;
drop table test_parquet_4;
drop table test_parquet_5;
drop table test_parquet_6;
drop table test_parquet_7;
