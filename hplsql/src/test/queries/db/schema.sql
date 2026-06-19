drop table if exists src_dt;

create table src_dt (
  c1  string,
  c2  varchar(30),
  c3  char(30),
  c4  tinyint,
  c5  smallint,
  c6  int,
  c7  bigint,
  c8  decimal(19,4),
  c9  float,
  c10 double,
  c11 date,
  c12 timestamp
);

insert overwrite table src_dt
select 
  value c1,
  value c2,
  value c3,
  cast(key as tinyint) c4,
  cast(key as smallint) c5,
  cast(key as int) c6,
  cast(key as bigint) c7,
  cast(key as decimal)/10 c8,
  cast(key as float)/10 c9,
  cast(key as double)/10 c10,
  date '2015-09-07' c11,
  cast(date '2015-09-07' as timestamp) c12
from src;

create table if not exists src_empty (
  c1 string)
;

create table if not exists src_insert (
  c1 string)
;