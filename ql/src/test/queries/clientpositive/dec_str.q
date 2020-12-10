create table t1 (a decimal (3,1));
explain select * from t1 where a = '22.3';
explain select * from t1 where a = '2.3';
explain select * from t1 where a = '213.223';
explain select * from t1 where a = '';
explain select * from t1 where a = 'ab';

set hive.strict.checks.type.safety=true;
create table t2 (a string);
insert into t2 values ('1208925742523269458163819');
select a from t2 where a=1208925742523269479013976;
explain cbo select a from t2 where a=1208925742523269479013976;
set hive.explain.user=false;
explain select a from t2 where a=1208925742523269479013976;
