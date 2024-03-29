--! qt:dataset:src

drop table if exists json_serde1_1;
drop table if exists json_serde1_2;
drop table if exists json_serde1_3;
drop table if exists json_serde1_4;

create table json_serde1_1 (a array<string>,b map<string,int>)
  row format serde 'org.apache.hive.hcatalog.data.JsonSerDe';

insert into table json_serde1_1
  select array('aaa'),map('aaa',1) from src limit 2;

select * from json_serde1_1;

create table json_serde1_2 (
  a array<int>,
  b map<int,date>,
  c struct<c1:int, c2:string, c3:array<string>, c4:map<string, int>, c5:struct<c5_1:string, c5_2:int>>
) row format serde 'org.apache.hadoop.hive.serde2.JsonSerDe';

insert into table json_serde1_2
  select
    array(3, 2, 1),
    map(1, date '2001-01-01', 2, null),
    named_struct(
      'c1', 123456,
      'c2', 'hello',
      'c3', array('aa', 'bb', 'cc'),
      'c4', map('abc', 123, 'xyz', 456),
      'c5', named_struct('c5_1', 'bye', 'c5_2', 88))
  from src limit 2;

select * from json_serde1_2;

create table json_serde1_3 (c1 int, c2 string) stored as jsonfile;
show create table json_serde1_3;

create table json_serde1_4 (a array<string>,b map<string,int>)
  row format serde 'org.apache.hive.hcatalog.data.JsonSerDe'
  WITH SERDEPROPERTIES ('serialization.encoding'='ISO8859_1');

insert into table json_serde1_4
  select array('MÃ¼ller'),map('MÃ¼ller',1) from src limit 2;

select * from json_serde1_4;

drop table json_serde1_1;
drop table json_serde1_2;
drop table json_serde1_3;
drop table json_serde1_4;
