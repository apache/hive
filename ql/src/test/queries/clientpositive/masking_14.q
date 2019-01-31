--! qt:dataset:srcpart
--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;

create database atlasmask;
use atlasmask;
create table masking_test_n8 (key int, value int);
insert into masking_test_n8 values(1,1), (2,2);
create view testv(c,d) as select * from masking_test_n8;

select * from `atlasmask`.`testv`;
select `testv`.`c` from `atlasmask`.`testv`
