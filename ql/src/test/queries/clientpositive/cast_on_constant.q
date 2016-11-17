create table t1(ts_field timestamp, date_field date);
explain select * from t1 where ts_field = "2016-01-23 00:00:00";
explain select * from t1 where date_field = "2016-01-23";
explain select * from t1 where ts_field = timestamp '2016-01-23 00:00:00';
explain select * from t1 where date_field = date '2016-01-23';
explain select * from t1 where date_field = ts_field;

drop table t1;
