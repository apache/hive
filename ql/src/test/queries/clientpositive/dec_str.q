create table t1 (a decimal (3,1));
explain select * from t1 where a = '22.3';
explain select * from t1 where a = '2.3';
explain select * from t1 where a = '213.223';
set hive.strict.checks.type.safety=false;
explain select * from t1 where a = '';
explain select * from t1 where a = 'ab';
