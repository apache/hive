set hive.security.authorization.enabled=true;
set hive.test.authz.sstd.hs2.mode=true;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;
set hive.repl.rootdir=${system:test.tmp.dir}/hrepl;

dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/hrepl/sentinel;
dfs -rmr  ${system:test.tmp.dir}/hrepl;
dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/hrepl;

set user.name=hive_admin_user;
set role ADMIN;

drop database if exists test_repldump_adminpriv cascade;

set user.name=ruser1;
show role grant user ruser1;

create database test_repldump_adminpriv;
create table test_repldump_adminpriv.dummy_tbl(a int) partitioned by (b string);
show tables test_repldump_adminpriv;

set user.name=hive_admin_user;
set role ADMIN;
show role grant user ruser1;
show role grant user hive_admin_user;

-- repl dump as admin should succeed
show tables test_repldump_adminpriv;
repl dump test_repldump_adminpriv;

dfs -rmr  ${system:test.tmp.dir}/hrepl/next;

set user.name=ruser1;
show tables test_repldump_adminpriv;

-- repl dump as non-admin should fail
repl dump test_repldump_adminpriv;
