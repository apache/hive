--! qt:authorizer

-- check insert without select priv
create table testp(i int) partitioned by (dt string);
grant select on table testp to user user1;

set user.name=user1;
create table user2tab(i int);
explain authorization insert into table testp partition (dt = '2012')  values (1);
explain authorization insert overwrite table testp partition (dt = '2012')  values (1);
insert into table testp partition (dt = '2012')  values (1);
insert overwrite table testp partition (dt = '2012')  values (1);
