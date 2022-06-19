--! qt:authorizer
set user.name=user1;

create temporary function or replace temp_fun as 'org.apache.hadoop.hive.ql.udf.UDFAscii';
create function or replace test_fun as 'org.apache.hadoop.hive.ql.udf.UDFAscii';

-- check if replace permanent function with new username fails

set user.name=user2;
create temporary function or replace temp_fun as 'org.apache.hadoop.hive.ql.udf.UDFAsin';
create function or replace test_fun as 'org.apache.hadoop.hive.ql.udf.UDFAsin';
