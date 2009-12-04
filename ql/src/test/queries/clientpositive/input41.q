set hive.mapred.mode=strict;

create table dest_sp (cnt int);

insert overwrite table dest_sp
select * from 
  (select count(1) as cnt from src 
    union all
   select count(1) as cnt from srcpart where ds = '2009-08-09'
  )x;

select * from dest_sp x order by x.cnt limit 2;

drop table dest_sp;
   
create table dest_sp (key string, val string);

insert overwrite table dest_sp
select * from 
  (select * from src 
    union all
   select * from srcpart where ds = '2009-08-09'
  )x;

select * from dest_sp x order by x.key limit 10000;

drop table dest_sp;
