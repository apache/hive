create database unicode_data_db;
use unicode_data_db;

-- test simple unicode data
create table t_test (name string, value int) ;
insert into table t_test VALUES ('李四', 100),('ちぱっち', 200);
select  name, value from t_test;
select name, value from t_test where name='李四';

-- test view with unicode predicate
create view t_view_test as select value from t_test where name='李四';
explain select * from t_view_test;
select  * from t_view_test;

drop database unicode_data_db cascade;
