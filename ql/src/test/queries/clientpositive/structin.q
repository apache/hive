create table t11_n1 (`id` string, `lineid` string);
set hive.cbo.enable=false;
set hive.tez.dynamic.partition.pruning=false;
set hive.vectorized.execution.enabled=true;

explain select * from t11_n1 where struct(`id`, `lineid`)
IN (
struct('1234-1111-0074578664','3'),
struct('1234-1111-0074578695','1'),
struct('1234-1111-0074580704','1'),
struct('1234-1111-0074581619','2'),
struct('1234-1111-0074582745','1'),
struct('1234-1111-0074586625','1'),
struct('1234-1111-0074019112','1'),
struct('1234-1111-0074019610','1'),
struct('1234-1111-0074022106','1')
);

explain select * from t11_n1 where struct(`id`, `lineid`)
IN (
struct('1234-1111-0074578664','3'),
struct('1234-1111-0074578695',1)
);

CREATE TABLE test_struct
(
  f1 string,
  demo_struct struct<f1:string, f2:string, f3:string>,
  datestr string
);

insert into test_struct values('s1', named_struct('f1','1', 'f2','2', 'f3','3'), '02-02-2020');
insert into test_struct values('s2', named_struct('f1',cast(null as string),'f2', cast(null as string),'f3', cast(null as string)), '02-02-2020');
insert into test_struct values('s4', named_struct('f1','100', 'f2','200', 'f3','300'), '02-02-2020');

explain select * from test_struct where datestr='02-02-2020' and demo_struct is not null
 order by f1;
select * from test_struct where datestr='02-02-2020' and demo_struct is not null
 order by f1;

DROP TABLE test_struct;