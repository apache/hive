--! qt:authorizer
set user.name=user1;

create temporary function temp_fun as 'org.apache.hadoop.hive.ql.udf.UDFAscii';
create function perm_fun as 'org.apache.hadoop.hive.ql.udf.UDFAscii';

-- check if permanent function with new username fails

set user.name=user2;
create temporary function temp_fun as 'org.apache.hadoop.hive.ql.udf.UDFAsin';
create function perm_fun as 'org.apache.hadoop.hive.ql.udf.UDFAsin';
