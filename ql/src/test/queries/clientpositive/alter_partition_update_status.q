create table src_stat_part_one(key string, value string) partitioned by (partitionId int);

insert overwrite table src_stat_part_one partition (partitionId=1)
  select * from src1;

ANALYZE TABLE src_stat_part_one PARTITION(partitionId=1) COMPUTE STATISTICS for columns;

describe formatted src_stat_part_one PARTITION(partitionId=1) key;

ALTER TABLE src_stat_part_one PARTITION(partitionId=1) UPDATE STATISTICS for column key SET ('numDVs'='11','avgColLen'='2.2');

describe formatted src_stat_part_one PARTITION(partitionId=1) key;

create table src_stat_part_two(key string, value string) partitioned by (px int, py string);

insert overwrite table src_stat_part_two partition (px=1, py='a')
  select * from src1;
  
ANALYZE TABLE src_stat_part_two PARTITION(px=1) COMPUTE STATISTICS for columns;

describe formatted src_stat_part_two PARTITION(px=1, py='a') key;

ALTER TABLE src_stat_part_two PARTITION(px=1, py='a') UPDATE STATISTICS for column key SET ('numDVs'='30','maxColLen'='40');

describe formatted src_stat_part_two PARTITION(px=1, py='a') key;

create database if not exists dummydb;

use dummydb;

ALTER TABLE default.src_stat_part_two PARTITION(px=1, py='a') UPDATE STATISTICS for column key SET ('numDVs'='40','maxColLen'='50');

describe formatted default.src_stat_part_two PARTITION(px=1, py='a') key;

use default;

drop database dummydb;
