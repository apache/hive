set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;
set hive.security.authorization.enabled=true;

-- check drop partition without delete privilege
create table tpart(i int, j int) partitioned by (k string);
alter table tpart add partition (k = 'abc') location 'file:${system:test.tmp.dir}/temp' ;
set user.name=user1;
alter table tpart drop partition (k = 'abc');
