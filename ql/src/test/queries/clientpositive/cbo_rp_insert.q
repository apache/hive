--! qt:disabled:disabled by 7e64114ddca5 in 2018
set hive.cbo.enable=true;
set hive.cbo.returnpath.hiveop=true;

drop database if exists x314 cascade;
create database x314;
use x314;
create table source_n1(s1 int, s2 int);
create table target1_n0(x int, y int, z int);

insert into source_n1(s2,s1) values(2,1);
-- expect source_n1 to contain 1 row (1,2)
select * from source_n1;
insert into target1_n0(z,x) select * from source_n1;
-- expect target1_n0 to contain 1 row (2,NULL,1)
select * from target1_n0;

drop database if exists x314 cascade;