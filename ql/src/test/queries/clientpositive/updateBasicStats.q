set hive.mapred.mode=nonstrict;

create table s as select * from src limit 10;

explain select * from s;

alter table s update statistics set('numRows'='12');

explain select * from s;

analyze table s compute statistics;

explain select * from s;

alter table s update statistics set('numRows'='1212', 'rawDataSize'='500500');

explain select * from s;

CREATE TABLE calendarp (`year` int)  partitioned by (p int);

insert into table calendarp partition (p=1) values (2010), (2011), (2012); 

explain select * from calendarp where p=1;

alter table calendarp partition (p=1) update statistics set('numRows'='1000020000', 'rawDataSize'='300040000');

explain select * from calendarp where p=1;

create table src_stat_part_two(key string, value string) partitioned by (px int, py string);

insert overwrite table src_stat_part_two partition (px=1, py='a')
  select * from src limit 1;

insert overwrite table src_stat_part_two partition (px=1, py='b')
  select * from src limit 10;

insert overwrite table src_stat_part_two partition (px=2, py='b')
  select * from src limit 100;

explain select * from src_stat_part_two where px=1 and py='a';

explain select * from src_stat_part_two where px=1;

alter table src_stat_part_two partition (px=1, py='a') update statistics set('numRows'='1000020000', 'rawDataSize'='300040000');

explain select * from src_stat_part_two where px=1 and py='a';

explain select * from src_stat_part_two where px=1;

alter table src_stat_part_two partition (px=1) update statistics set('numRows'='1000020000', 'rawDataSize'='300040000');

explain select * from src_stat_part_two where px=1 and py='a';

explain select * from src_stat_part_two where px=1;
