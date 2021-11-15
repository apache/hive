--! qt:authorizer
set user.name=user1;

-- check if alter table fails as different user
create table t1(i int);

set user.name=user2;
ALTER TABLE t1 SET SERDEPROPERTIES ('field.delim' = ',');
