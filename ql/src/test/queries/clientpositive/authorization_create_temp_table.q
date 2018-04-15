--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.test.authz.sstd.hs2.mode=true;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;

create table authorization_create_temp_table_1 as select * from src limit 10;
grant select on authorization_create_temp_table_1 to user user1;

set user.name=user1;
set hive.security.authorization.enabled=true;

create temporary table tmp1(c1 string, c2 string);

insert overwrite table tmp1 select * from authorization_create_temp_table_1;

select c1, count(*) from tmp1 group by c1 order by c1;

drop table tmp1;
