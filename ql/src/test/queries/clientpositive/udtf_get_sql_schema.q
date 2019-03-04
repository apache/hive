set hive.fetch.task.conversion=more;
set hive.mapred.mode=nonstrict;

describe function get_sql_schema;
describe function extended get_sql_schema;

create table t1(c1 int, c2 float, c3 double, c4 string, c5 date, c6 array<int>, c7 struct<a:int,b:string>, c8 map<int,int>);
insert into t1 select 1, 1.1, 2.2, 'val1', '2019-02-15', array(1), named_struct('a',1,'b','2'), map(1,1);

explain select get_sql_schema('select * from t1');
select get_sql_schema('select * from t1');

create external table t2(c1 int, c2 float, c3 double, c4 string, c5 date, c6 array<int>, c7 struct<a:int,b:string>, c8 map<int,int>);
insert into t2 select 1, 1.1, 2.2, 'val1', '2019-02-15', array(1), named_struct('a',1,'b','2'), map(1,1);

explain select get_sql_schema('select * from t2');
select get_sql_schema('select * from t2');
