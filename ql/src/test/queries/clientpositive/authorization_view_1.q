--! qt:dataset:src
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;

create table src_autho_test_n8 as select * from src;

create view v_n9 as select * from src_autho_test_n8;

create view v1_n13 as select * from src_autho_test_n8;

create view v2_n7 as select * from src_autho_test_n8;

set hive.security.authorization.enabled=true;

--table grant to user

grant select on table src_autho_test_n8 to user hive_test_user;

grant select on table v_n9 to user hive_test_user;
grant select on table v1_n13 to user hive_test_user;
grant select on table v2_n7 to user hive_test_user;

show grant user hive_test_user on table v_n9;
show grant user hive_test_user on v_n9;
show grant user hive_test_user on v_n9(key);

select * from v_n9 order by key limit 10;

revoke select on table src_autho_test_n8 from user hive_test_user;

show grant user hive_test_user on table v_n9;
show grant user hive_test_user on v_n9;
show grant user hive_test_user on v_n9(key);

revoke select on table v_n9 from user hive_test_user;

show grant user hive_test_user on table v_n9;
show grant user hive_test_user on v_n9;
show grant user hive_test_user on v_n9(key);

--column grant to user

grant select on table src_autho_test_n8 to user hive_test_user;
grant select(key) on table v_n9 to user hive_test_user;

show grant user hive_test_user on table v_n9;
show grant user hive_test_user on v_n9(key);

select key from v_n9 order by key limit 10;

select key from
(select v_n9.key from src_autho_test_n8 join v_n9 on src_autho_test_n8.key=v_n9.key)subq 
order by key limit 10;

select key from
(select key as key from src_autho_test_n8 union all select key from v_n9)subq 
limit 10;

select key from
(select value as key from v2_n7 union select value as key from v1_n13 union all select key from v_n9)subq
limit 10;
