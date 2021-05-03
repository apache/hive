create table table_map_types (id int, c1 map<int,int>, c2 map<int,int>);
set hive.cbo.enable=false;
set hive.auto.convert.join=false;
set hive.optimize.ppd=false;
select * from table_map_types t1 inner join table_map_types t2 on t1.c1 = t2.c1;
