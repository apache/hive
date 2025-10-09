--! qt:replace:/(\b2016\b.{1,17}\s\S*\s)/#Masked#/

create table t1 (
  t tinyint default 1Y,
  si smallint default 1S,
  i int default 1,
  b bigint default 1L,
  f double default double(5.7),
  d double,
  s varchar(25) default cast('col1' as varchar(25)),
  dc decimal(38,18),
  bo varchar(5),
  v varchar(25),
  c char(25) default cast('var1' as char(25)),
  ts timestamp DEFAULT TIMESTAMP'2016-02-22 12:45:07.000000000',
  dt date default cast('2015-03-12' as DATE),
  tz timestamp with local time zone DEFAULT TIMESTAMPLOCALTZ'2016-01-03 12:26:34 America/Los_Angeles')
STORED AS TEXTFILE;

insert into t1(t,si) values (2,5);
insert into t1(b,dt) values (2,cast('2019-08-14' as DATE));
select tz,b,dt,t,si from t1 ORDER BY t1.t;