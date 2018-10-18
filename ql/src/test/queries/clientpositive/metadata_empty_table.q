set hive.compute.query.using.stats=false;
set hive.fetch.task.conversion=none;

drop table nullscan_table;
drop table onerow_table;
create table nullscan_table(i int); 
create table onerow_table(i int); 
insert into table onerow_table values(0);

set hive.optimize.metadataonly=true;
select (1=1) from nullscan_table group by (1=1); 
select (1=1) from onerow_table group by (1=1); 


set hive.optimize.metadataonly=false;
select (1=1) from nullscan_table group by (1=1); 
select (1=1) from onerow_table group by (1=1); 


drop table nullscan_table;
drop table onerow_table;

