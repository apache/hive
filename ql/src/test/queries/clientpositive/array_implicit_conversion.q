drop table if exists aic_tab;
create table aic_tab(array_type array <char(10)>);
insert into table aic_tab select array(cast ('a' as char(10)));
select * from aic_tab;

drop table if exists aic_tab;
create table aic_tab (c1 array<smallint>);
insert into aic_tab(c1) values (array(55,54));
select * from aic_tab;

drop table if exists aic_tab;
create table aic_tab (c1 array<array<smallint>>);
insert into aic_tab(c1) values (array(array(44,45),array(44,45)));
select * from aic_tab;

drop table if exists aic_tab;
create table aic_tab (c1 array<date>);
insert into aic_tab(c1) values(array('2006-09-25','1980-07-19','1992-02-11'));
select * from aic_tab;

drop table if exists aic_tab;
create table aic_tab (c1 array<date>);
insert into aic_tab(c1) values(array('2002-hi-25','1980-07-19','1992-pi-11'));
select * from aic_tab;
