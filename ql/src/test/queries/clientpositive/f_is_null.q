create table t (a string); insert into t values (null),('1988-11-11');

set hive.cbo.enable=true;
select 'expected 1 (to_date)',count(1) from t where to_date(a) is null;
select 'expected 1 (second)', count(1) from t where second(a) is null;

set hive.cbo.enable=false;
select 'expected 1 (to_date)',count(1) from t where to_date(a) is null;
select 'expected 1 (second)', count(1) from t where second(a) is null;

