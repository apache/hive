set hive.fetch.task.conversion=none;

drop table decimal_test;
create table decimal_test (dc decimal(38,18));
insert into table decimal_test values (4327269606205.029297);

explain 
select * from decimal_test where dc = 4327269606205.029297;
select * from decimal_test where dc = 4327269606205.029297;

set hive.cbo.enable=false;
select * from decimal_test where dc = 4327269606205.029297;

drop table decimal_test;
