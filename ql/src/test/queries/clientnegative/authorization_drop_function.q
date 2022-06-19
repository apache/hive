--! qt:authorizer
set user.name=user1;
create function or replace test_fun as 'org.apache.hadoop.hive.ql.udf.UDFAscii';

-- only owner of permanent function can drop function

set user.name=user2;
drop function test_fun;
