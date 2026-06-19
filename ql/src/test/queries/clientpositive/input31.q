--! qt:disabled:disabled by 54b4b2d428bb in 2018
--! qt:dataset:srcbucket
set hive.mapred.mode=nonstrict;



set hive.test.mode=true;
set hive.test.mode.prefix=tst_;

create table tst_dest31(a int);
create table dest31(a int);

explain 
insert overwrite table dest31
select count(1) from srcbucket;       

insert overwrite table dest31
select count(1) from srcbucket;       

set hive.test.mode=false;

select * from tst_dest31;
