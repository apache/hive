--! qt:dataset:src
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;
set hive.cbo.enable=false;
set hive.exec.reducers.max=1;

create table src_autho_test_n9 as select * from src;

create view v_n10 as select * from src_autho_test_n9;

create view v1_n14 as select * from src_autho_test_n9;

create view v2_n8 as select * from src_autho_test_n9;

set hive.security.authorization.enabled=true;

--table grant to user

grant select on table src_autho_test_n9 to user hive_test_user;

grant select on table v_n10 to user hive_test_user;
grant select on table v1_n14 to user hive_test_user;
grant select on table v2_n8 to user hive_test_user;

show grant user hive_test_user on table v_n10;
show grant user hive_test_user on v_n10;
show grant user hive_test_user on v_n10(key);

select * from v_n10 order by key limit 10;

revoke select on table src_autho_test_n9 from user hive_test_user;

show grant user hive_test_user on table v_n10;
show grant user hive_test_user on v_n10;
show grant user hive_test_user on v_n10(key);

revoke select on table v_n10 from user hive_test_user;

show grant user hive_test_user on table v_n10;
show grant user hive_test_user on v_n10;
show grant user hive_test_user on v_n10(key);

--column grant to user

grant select on table src_autho_test_n9 to user hive_test_user;
grant select(key) on table v_n10 to user hive_test_user;

show grant user hive_test_user on table v_n10;
show grant user hive_test_user on v_n10(key);

select key from v_n10 order by key limit 10;

select key from
(select v_n10.key from src_autho_test_n9 join v_n10 on src_autho_test_n9.key=v_n10.key)subq 
order by key limit 10;

select key from
(select key as key from src_autho_test_n9 union all select key from v_n10)subq 
limit 10;

select key from
(select value as key from v2_n8 union select value as key from v1_n14 union all select key from v_n10)subq
limit 10;

set hive.cbo.enable=true;

--although cbo is enabled, it will not succeed.

select key from v_n10 cluster by key limit 10;

select key from
(select key as key from src_autho_test_n9 union all select key from v_n10 cluster by key)subq
limit 10;
