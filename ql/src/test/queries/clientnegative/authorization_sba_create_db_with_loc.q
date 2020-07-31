set hive.security.authorization.enabled=true;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.StorageBasedAuthorizationProvider;

-- try to create database where we don't have write permissions and this fails
-- In logs, it generates exception like below.

-- org.apache.hadoop.hive.ql.metadata.HiveException: java.security.AccessControlException:
-- Permission denied: user=hive_test_user,
-- path="file:/Users/.../hive/itests/qtest/target/tmp/databases_no_write_permissions":schaurasia:staff:dr-xr-xr-x

dfs -mkdir -p ${system:test.tmp.dir}/databases_no_write_permissions;
dfs -chmod 555 ${system:test.tmp.dir}/databases_no_write_permissions;

create database d1 location '${system:test.tmp.dir}/databases_no_write_permissions/d1';