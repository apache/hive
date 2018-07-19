set hive.mapred.mode=nonstrict;
set hive.cbo.enable=true;

create table a_n7(key int, value int);

insert into table a_n7 values (1,2),(1,2),(1,3),(2,3);

create table b_n5(key int, value int);

insert into table b_n5 values (1,2),(2,3);

explain select * from b_n5 intersect distinct select * from a_n7 intersect distinct select * from b_n5 intersect distinct select * from a_n7 intersect distinct select * from b_n5;

explain (select * from b_n5 intersect distinct select * from a_n7) intersect distinct (select * from b_n5 intersect distinct select * from a_n7);

explain select * from b_n5 intersect distinct (select * from a_n7 intersect distinct (select * from b_n5 intersect distinct (select * from a_n7 intersect distinct select * from b_n5)));

explain (((select * from b_n5 intersect distinct select * from a_n7) intersect distinct select * from b_n5) intersect distinct select * from a_n7) intersect distinct select * from b_n5;

explain select * from b_n5 intersect distinct (select * from a_n7 intersect distinct select * from b_n5) intersect distinct select * from a_n7 intersect distinct select * from b_n5;

explain select * from b_n5 intersect distinct (select * from a_n7 intersect all select * from b_n5);

explain select * from b_n5 intersect all (select * from a_n7 intersect all select * from b_n5);

explain select * from b_n5 intersect all (select * from a_n7 intersect distinct select * from b_n5);

