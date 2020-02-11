--! qt:authorizer
set user.name=hive_test_user;

-- permanent function creation should fail for non-admin roles
create function perm_fn as 'org.apache.hadoop.hive.ql.udf.UDFAscii';
