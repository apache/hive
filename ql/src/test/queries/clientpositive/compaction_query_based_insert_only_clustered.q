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

DROP TABLE IF EXISTS orc_bucketed;

create table orc_bucketed (a int, b string) clustered by (a) into 3 buckets stored as orc TBLPROPERTIES('transactional'='true', 'transactional_properties'='insert_only');

insert into orc_bucketed values('1', 'text1');
insert into orc_bucketed values('2', 'text2');
insert into orc_bucketed values('3', 'text3');
insert into orc_bucketed values('4', 'text4');
insert into orc_bucketed values('5', 'text5');
insert into orc_bucketed values('6', 'text6');
insert into orc_bucketed values('7', 'text7');
insert into orc_bucketed values('8', 'text8');
insert into orc_bucketed values('9', 'text9');
insert into orc_bucketed values('10', 'text10');

describe extended orc_bucketed;
alter table orc_bucketed compact 'MAJOR' and wait;
analyze table orc_bucketed compute statistics;

describe extended orc_bucketed;
