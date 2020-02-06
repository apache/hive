--! qt:authorizer
set user.name=hive_admin_user;

-- admin required for create function
set role ADMIN;

create temporary function temp_fn as 'org.apache.hadoop.hive.ql.udf.UDFAscii';
create function perm_fn as 'org.apache.hadoop.hive.ql.udf.UDFAscii';

drop temporary function temp_fn;
drop function perm_fn;
