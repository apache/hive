set hive.test.authz.sstd.hs2.mode=true;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;
set hive.security.authorization.enabled=true;

dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/hive-12875-export/temp;
dfs -rmr ${system:test.tmp.dir}/hive-12875-export;
dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/hive-12875-export/;

-- check export on partition
create table auth_export_ptn(i int) partitioned by (j int);
alter table auth_export_ptn add partition (j=42);
set user.name=user1;
export table auth_export_ptn partition (j=42) to 'pfile://${system:test.tmp.dir}/hive-12875-export';

set hive.security.authorization.enabled=false;

drop table auth_export_ptn;

