--! qt:replace:/createTime:(\d+)/#Masked#/
--! qt:replace:/location:(\S+)/#Masked#/
--! qt:replace:/lastAccessTime:(\d+)/#Masked#/
--! qt:replace:/ownerType:(\S*)/#Masked#/
--! qt:replace:/owner:(\S*)/#Masked#/
--! qt:replace:/skewedColValueLocationMaps:(\S*)/#Masked#/
--! qt:replace:/transient_lastDdlTime=(\d+)/#Masked#/
--! qt:replace:/rawDataSize=(\d+)/#Masked#/
--! qt:replace:/writeId:(\d+)/#Masked#/
--! qt:replace:/bucketing_version=(\d+)/#Masked#/
--! qt:replace:/id:(\d+)/#Masked#/

DROP TABLE IF EXISTS orc_partition;

CREATE TABLE orc_partition
(a STRING, b STRING)
PARTITIONED BY (c STRING)
STORED AS ORC TBLPROPERTIES('transactional'='true');

insert into orc_partition values('1', 'text1', 'part1');
insert into orc_partition values('2', 'text2', 'part1');
insert into orc_partition values('3', 'text3', 'part2');
insert into orc_partition values('4', 'text4', 'part2');
insert into orc_partition values('5', 'text5', 'part3');
insert into orc_partition values('6', 'text6', 'part3');
insert into orc_partition values('7', 'text7', 'part4');
insert into orc_partition values('8', 'text8', 'part4');
insert into orc_partition values('9', 'text9', 'part5');
insert into orc_partition values('10', 'text10', 'part5');

describe extended orc_partition;

alter table orc_partition PARTITION (c='part1') compact 'MAJOR' and wait;
alter table orc_partition PARTITION (c='part2') compact 'MAJOR' and wait;
alter table orc_partition PARTITION (c='part3') compact 'MAJOR' and wait;
alter table orc_partition PARTITION (c='part4') compact 'MAJOR' and wait;
alter table orc_partition PARTITION (c='part5') compact 'MAJOR' and wait;

analyze table orc_partition compute statistics;
describe extended orc_partition;
