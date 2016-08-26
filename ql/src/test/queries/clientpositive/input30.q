set hive.mapred.mode=nonstrict;




create table dest30(a int);
create table tst_dest30(a int);

set hive.test.mode=true;
set hive.test.mode.prefix=tst_;

explain 
insert overwrite table dest30
select count(1) from src;       

insert overwrite table dest30
select count(1) from src;       

set hive.test.mode=false;

select * from tst_dest30;

set hive.cbo.returnpath.hiveop=true;

explain
insert overwrite table dest30
select count(1) from src;

insert overwrite table dest30
select count(1) from src;

set hive.test.mode=false;

select * from tst_dest30;

set hive.cbo.returnpath.hiveop=false;


