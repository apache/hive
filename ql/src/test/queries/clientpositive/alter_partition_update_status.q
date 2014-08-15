create table src_stat_part_one(key string, value string) partitioned by (partitionId int);

insert overwrite table src_stat_part_one partition (partitionId=1)
  select * from src1;

ANALYZE TABLE src_stat_part_one PARTITION(partitionId=1) COMPUTE STATISTICS for columns;

describe formatted src_stat_part_one.key PARTITION(partitionId=1);

ALTER TABLE src_stat_part_one PARTITION(partitionId=1) UPDATE STATISTICS for column key SET ('numDVs'='11','avgColLen'='2.2');

describe formatted src_stat_part_one.key PARTITION(partitionId=1);

create table src_stat_part_two(key string, value string) partitioned by (px int, py string);

insert overwrite table src_stat_part_two partition (px=1, py='a')
  select * from src1;
  
ANALYZE TABLE src_stat_part_two PARTITION(px=1) COMPUTE STATISTICS for columns;

describe formatted src_stat_part_two.key PARTITION(px=1, py='a');

ALTER TABLE src_stat_part_two PARTITION(px=1, py='a') UPDATE STATISTICS for column key SET ('numDVs'='30','maxColLen'='40');

describe formatted src_stat_part_two.key PARTITION(px=1, py='a');