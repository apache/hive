SET hive.cli.print.header=true;

explain cbo
select cast(null as map<int, string>), cast(null as array<map<int, string>>), cast(null as int), cast(null as struct<f1:array<map<int, string>>, f2:struct<a:double, b:string>>);
explain
select cast(null as map<int, string>), cast(null as array<map<int, string>>), cast(null as int), cast(null as struct<f1:array<map<int, string>>, f2:struct<a:double, b:string>>);
select cast(null as map<int, string>), cast(null as array<map<int, string>>), cast(null as int), cast(null as struct<f1:array<map<int, string>>, f2:struct<a:double, b:string>>);


create table t1 as
select cast(null as map<int, string>), cast(null as array<map<int, string>>), cast(null as int), cast(null as struct<f1:array<map<int, string>>, f2:struct<a:double, b:string>>);

describe formatted t1;
