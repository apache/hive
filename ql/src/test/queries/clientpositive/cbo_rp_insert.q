set hive.cbo.enable=true;
set hive.cbo.returnpath.hiveop=true;

drop database if exists x314 cascade;
create database x314;
use x314;
create table source(s1 int, s2 int);
create table target1(x int, y int, z int);

insert into source(s2,s1) values(2,1);
-- expect source to contain 1 row (1,2)
select * from source;
insert into target1(z,x) select * from source;
-- expect target1 to contain 1 row (2,NULL,1)
select * from target1;

drop database if exists x314 cascade;