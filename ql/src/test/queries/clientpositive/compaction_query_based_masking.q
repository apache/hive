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


set hive.mapred.mode=nonstrict;
set hive.security.authorization.enabled=true;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;

drop table masking_test_n_compact;

create table masking_test_n_compact (key int, value string) stored as orc TBLPROPERTIES('transactional'='true');



insert into masking_test_n_compact values('1', 'text1');
insert into masking_test_n_compact values('2', 'text2');
insert into masking_test_n_compact values('3', 'text3');

alter table masking_test_n_compact compact 'MAJOR' and wait;

select * from masking_test_n_compact;
