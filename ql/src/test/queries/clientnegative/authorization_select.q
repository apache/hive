--! qt:authorizer

-- check query without select privilege fails
create table t1(i int);

set user.name=user1;
select * from t1;
