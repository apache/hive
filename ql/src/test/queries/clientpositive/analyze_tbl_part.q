create table src_stat_part(key string, value string) partitioned by (partitionId int);

insert overwrite table src_stat_part partition (partitionId=1)
select * from src1;

insert overwrite table src_stat_part partition (partitionId=2)
select * from src1;

ANALYZE TABLE src_stat_part partition (partitionId) COMPUTE STATISTICS for columns key;

describe formatted src_stat_part.key PARTITION(partitionId=1);

ANALYZE TABLE src_stat_part partition (partitionId) COMPUTE STATISTICS for columns key, value;

describe formatted src_stat_part.key PARTITION(partitionId=1);

describe formatted src_stat_part.value PARTITION(partitionId=2);

create table src_stat_string_part(key string, value string) partitioned by (partitionName string);

insert overwrite table src_stat_string_part partition (partitionName="p'1")
select * from src1;

insert overwrite table src_stat_string_part partition (partitionName="p\"1")
select * from src1;

ANALYZE TABLE src_stat_string_part partition (partitionName="p'1") COMPUTE STATISTICS for columns key, value;

ANALYZE TABLE src_stat_string_part partition (partitionName="p\"1") COMPUTE STATISTICS for columns key, value;