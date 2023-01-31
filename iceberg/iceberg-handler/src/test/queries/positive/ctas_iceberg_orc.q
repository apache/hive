set hive.explain.user=false;
set iceberg.mr.schema.auto.conversion=true;

create table source(
  i int,
  s string,
  vc varchar(256),
  c char(10),
  t tinyint,
  si smallint);

insert into source values (1, 'one', 'one_1', 'ch_1', 10, 11);
insert into source values (1, 'two', 'two_2', 'ch_2', 20, 22);

explain
create external table tbl_ice stored by iceberg stored as orc tblproperties ('format-version'='2') as
select i, s, vc,c, t, si from source;

create external table tbl_ice stored by iceberg stored as orc tblproperties ('format-version'='2') as
select i, s, vc,c, t, si from source;

select * from tbl_ice;


-- Test insert - select
explain
insert into tbl_ice
select * from source;

insert into tbl_ice
select * from source;

select * from tbl_ice;


-- Test insert overwrite
explain
insert overwrite table tbl_ice
select * from source;

insert overwrite table tbl_ice
select * from source;

select * from tbl_ice;
