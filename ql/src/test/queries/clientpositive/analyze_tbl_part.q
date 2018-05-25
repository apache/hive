set hive.mapred.mode=nonstrict;
create table src_stat_part_n1(key string, value string) partitioned by (partitionId int);

insert overwrite table src_stat_part_n1 partition (partitionId=1)
select * from src1;

insert overwrite table src_stat_part_n1 partition (partitionId=2)
select * from src1;

ANALYZE TABLE src_stat_part_n1 partition (partitionId) COMPUTE STATISTICS for columns key;

describe formatted src_stat_part_n1 PARTITION(partitionId=1) key;

ANALYZE TABLE src_stat_part_n1 partition (partitionId) COMPUTE STATISTICS for columns key, value;

describe formatted src_stat_part_n1 PARTITION(partitionId=1) key;

describe formatted src_stat_part_n1 PARTITION(partitionId=2) value;

create table src_stat_string_part(key string, value string) partitioned by (partitionName string);

insert overwrite table src_stat_string_part partition (partitionName="p\'1")
select * from src1;

insert overwrite table src_stat_string_part partition (partitionName="p\"1")
select * from src1;

ANALYZE TABLE src_stat_string_part partition (partitionName="p\'1") COMPUTE STATISTICS for columns key, value;

ANALYZE TABLE src_stat_string_part partition (partitionName="p\"1") COMPUTE STATISTICS for columns key, value;

-- analyze table without specifying partition spec
ANALYZE TABLE src_stat_string_part COMPUTE STATISTICS for columns key, value;
