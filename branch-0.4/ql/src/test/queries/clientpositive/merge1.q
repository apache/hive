drop table dest1;
set hive.merge.mapredfiles=true;

create table dest1(key int, val int);

explain
insert overwrite table dest1
select key, count(1) from src group by key;

insert overwrite table dest1
select key, count(1) from src group by key;

select * from dest1;

drop table dest1;
