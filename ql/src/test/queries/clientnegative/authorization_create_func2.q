--! qt:authorizer
set user.name=hive_test_user;

-- temp function creation should fail for non-admin roles
create temporary function temp_fn as 'org.apache.hadoop.hive.ql.udf.UDFAscii';

