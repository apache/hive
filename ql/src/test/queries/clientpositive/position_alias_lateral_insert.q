-- HIVE-30037: CBO's Hive operator return path declines lateral views.
set hive.cbo.returnpath.hiveop=true;
create table pali_src (a int, b int);
insert into pali_src values (1,30), (2,10), (3,20);
create table pali_dst (a int, b int);

insert into pali_dst (b,a)
select t.c,b from pali_src lateral view explode(array(a,-a)) t as c order by 1 desc limit 1;
select 'order', a,b from pali_dst;

truncate table pali_dst;
insert into pali_dst (b,a)
select t.c,b from pali_src lateral view explode(array(a,-a)) t as c distribute by 2 sort by 1 desc limit 1;
select 'distribute_sort', a,b from pali_dst;

truncate table pali_dst;
insert into pali_dst (b,a)
select t.c,b from pali_src lateral view explode(array(a,-a)) t as c cluster by 1;
select 'cluster', a,b from pali_dst order by b;

truncate table pali_dst;
insert into pali_dst (b)
select t.c from pali_src lateral view explode(array(a,-a)) t as c order by 1 desc limit 1;
select 'missing', a,b from pali_dst;
