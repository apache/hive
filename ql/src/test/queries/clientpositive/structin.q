create table t11 (`id` string, `lineid` string);
set hive.cbo.enable=false;
set hive.tez.dynamic.partition.pruning=false;
set hive.vectorized.execution.enabled=true;

explain select * from t11 where struct(`id`, `lineid`)
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

explain select * from t11 where struct(`id`, `lineid`)
IN (
struct('1234-1111-0074578664','3'),
struct('1234-1111-0074578695',1)
);
