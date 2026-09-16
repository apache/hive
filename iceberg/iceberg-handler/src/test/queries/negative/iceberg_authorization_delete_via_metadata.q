set hive.test.authz.sstd.hs2.mode=true;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;
set hive.security.authorization.enabled=true;

set user.name=owner_user;

drop table if exists ice_tbl;

create table ice_tbl (id int, val string) partitioned by (p int) stored by iceberg stored as orc tblproperties ('format-version'='2');

insert into ice_tbl values 
(1, 'a', 10), (2, 'b', 10), (3, 'c', 20), (4, 'd', 20);

grant select on table ice_tbl to user read_only_user;

set user.name=read_only_user;
set hive.optimize.delete.metadata.only=true;

delete from ice_tbl where p = 10;
