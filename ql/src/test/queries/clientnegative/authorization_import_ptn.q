set hive.test.authz.sstd.hs2.mode=true;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;
set hive.security.authorization.enabled=true;

dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/hive-12875-import/temp;
dfs -rmr ${system:test.tmp.dir}/hive-12875-import;
dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/hive-12875-import/;

-- check export on partition
create table auth_import_ptn(i int) partitioned by (j int);
alter table auth_import_ptn add partition (j=42);
export table auth_import_ptn partition (j=42) to 'pfile://${system:test.tmp.dir}/hive-12875-import';

alter table auth_import_ptn drop partition (j=42);

set user.name=user1;
import table auth_import_ptn partition (j=42) from 'pfile://${system:test.tmp.dir}/hive-12875-import';

set hive.security.authorization.enabled=false;

drop table auth_import_ptn;

