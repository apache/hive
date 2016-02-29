set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;
set hive.cbo.enable=false;

create table src_autho_test as select * from src;

create view v as select * from src_autho_test;

create view v1 as select * from src_autho_test;

create view v2 as select * from src_autho_test;

set hive.security.authorization.enabled=true;

--table grant to user

grant select on table src_autho_test to user hive_test_user;

grant select on table v to user hive_test_user;
grant select on table v1 to user hive_test_user;
grant select on table v2 to user hive_test_user;

show grant user hive_test_user on table v;
show grant user hive_test_user on v;
show grant user hive_test_user on v(key);

select * from v order by key limit 10;

revoke select on table src_autho_test from user hive_test_user;

show grant user hive_test_user on table v;
show grant user hive_test_user on v;
show grant user hive_test_user on v(key);

revoke select on table v from user hive_test_user;

show grant user hive_test_user on table v;
show grant user hive_test_user on v;
show grant user hive_test_user on v(key);

--column grant to user

grant select on table src_autho_test to user hive_test_user;
grant select(key) on table v to user hive_test_user;

show grant user hive_test_user on table v;
show grant user hive_test_user on v(key);

select key from v order by key limit 10;

select key from
(select v.key from src_autho_test join v on src_autho_test.key=v.key)subq 
order by key limit 10;

select key from
(select key as key from src_autho_test union all select key from v)subq 
limit 10;

select key from
(select value as key from v2 union select value as key from v1 union all select key from v)subq
limit 10;

set hive.cbo.enable=true;

--although cbo is enabled, it will not succeed.

select key from v sort by key limit 10;

select key from
(select key as key from src_autho_test union all select key from v cluster by key)subq
limit 10;
