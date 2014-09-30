set hive.metastore.pre.event.listeners=org.apache.hadoop.hive.ql.security.authorization.AuthorizationPreEventListener;
set hive.security.metastore.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.StorageBasedAuthorizationProvider;

dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/a_sba_droptab1;

create table t1(i int) location '${system:test.tmp.dir}/a_sba_droptab1';
dfs -chmod 555 ${system:test.tmp.dir}/a_sba_droptab1;
-- Attempt to drop table without having write permissions on table dir should result in error
drop table t1;
