--! qt:authorizer
set user.name=user1;

-- check if alter table fails as different user
create table t1(i int);
msck repair table t1;


set user.name=user1;
GRANT INSERT ON t1 TO USER user2;

set user.name=user2;
msck repair table t1;

set user.name=user3;
msck repair table t1;

