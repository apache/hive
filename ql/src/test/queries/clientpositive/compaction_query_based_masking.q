set hive.mapred.mode=nonstrict;
set hive.security.authorization.enabled=true;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;

drop table masking_test_n_compact;
drop table check_real_data;

create table masking_test_n_compact (key int, value string) stored as orc TBLPROPERTIES('transactional'='true');

insert into masking_test_n_compact values('1', 'text1');
insert into masking_test_n_compact values('2', 'text2');
insert into masking_test_n_compact values('3', 'text3');

select * from masking_test_n_compact;

-- the rules are applied based on the table name
alter table masking_test_n_compact rename to check_real_data;

select * from check_real_data;

alter table check_real_data rename to masking_test_n_compact;

alter table masking_test_n_compact compact 'MAJOR' and wait;

select * from masking_test_n_compact;

alter table masking_test_n_compact rename to check_real_data;

select * from check_real_data;
