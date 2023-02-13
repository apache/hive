set hive.map.aggr=false;

drop table tb1;

create table tb1 (key int, startTime string, endTime string);

insert into tb1 values (1, "100", "101");
insert into tb1 values (2, "200", "201");

select key, collect_list(map("startTime",startTime,"endTime",endTime)) as col1 from tb1 group by key order by key;