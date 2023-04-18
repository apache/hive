--! qt:replace:/createTime:(\d+)/#Masked#/
--! qt:replace:/location:(\S+)/#Masked#/
--! qt:replace:/lastAccessTime:(\d+)/#Masked#/
--! qt:replace:/ownerType:(\S*)/#Masked#/
--! qt:replace:/owner:(\S*)/#Masked#/
--! qt:replace:/skewedColValueLocationMaps:(\S*)/#Masked#/
--! qt:replace:/transient_lastDdlTime=(\d+)/#Masked#/
--! qt:replace:/totalSize=(\d+)/#Masked#/
--! qt:replace:/rawDataSize=(\d+)/#Masked#/
--! qt:replace:/writeId:(\d+)/#Masked#/
--! qt:replace:/bucketing_version=(\d+)/#Masked#/
--! qt:replace:/id:(\d+)/#Masked#/

DROP TABLE IF EXISTS orc_partition_clustered;

CREATE TABLE orc_partition_clustered
(a STRING, b STRING)
PARTITIONED BY (c STRING)
CLUSTERED by (a) INTO 3 buckets
STORED AS ORC TBLPROPERTIES('transactional'='true', 'transactional_properties'='insert_only');

insert into orc_partition_clustered values('1', 'text1', 'part1');
insert into orc_partition_clustered values('2', 'text2', 'part1');
insert into orc_partition_clustered values('3', 'text3', 'part2');
insert into orc_partition_clustered values('4', 'text4', 'part2');
insert into orc_partition_clustered values('5', 'text5', 'part3');
insert into orc_partition_clustered values('6', 'text6', 'part3');
insert into orc_partition_clustered values('7', 'text7', 'part4');
insert into orc_partition_clustered values('8', 'text8', 'part4');
insert into orc_partition_clustered values('9', 'text9', 'part5');
insert into orc_partition_clustered values('10', 'text10', 'part5');

describe extended orc_partition_clustered;

alter table orc_partition_clustered PARTITION (c='part1') compact 'MINOR' and wait;
alter table orc_partition_clustered PARTITION (c='part2') compact 'MINOR' and wait;
alter table orc_partition_clustered PARTITION (c='part3') compact 'MINOR' and wait;
alter table orc_partition_clustered PARTITION (c='part4') compact 'MINOR' and wait;
alter table orc_partition_clustered PARTITION (c='part5') compact 'MINOR' and wait;

analyze table orc_partition_clustered compute statistics;
describe extended orc_partition_clustered;
