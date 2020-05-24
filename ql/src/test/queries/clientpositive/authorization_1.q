--! qt:dataset:src
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;

-- SORT_BEFORE_DIFF

create table src_autho_test_n11 as select * from src;

set hive.security.authorization.enabled=true;

--table grant to user

grant select on table src_autho_test_n11 to user hive_test_user;

show grant user hive_test_user on table src_autho_test_n11;
show grant user hive_test_user on table src_autho_test_n11(key);

select key from src_autho_test_n11 order by key limit 20;

revoke select on table src_autho_test_n11 from user hive_test_user;
show grant user hive_test_user on table src_autho_test_n11;
show grant user hive_test_user on table src_autho_test_n11(key);

--column grant to user

grant select(key) on table src_autho_test_n11 to user hive_test_user;

show grant user hive_test_user on table src_autho_test_n11;
show grant user hive_test_user on table src_autho_test_n11(key);

select key from src_autho_test_n11 order by key limit 20;

revoke select(key) on table src_autho_test_n11 from user hive_test_user;
show grant user hive_test_user on table src_autho_test_n11;
show grant user hive_test_user on table src_autho_test_n11(key); 

--table grant to group

grant select on table src_autho_test_n11 to group hive_test_group1;

show grant group hive_test_group1 on table src_autho_test_n11;
show grant group hive_test_group1 on table src_autho_test_n11(key);

select key from src_autho_test_n11 order by key limit 20;

revoke select on table src_autho_test_n11 from group hive_test_group1;
show grant group hive_test_group1 on table src_autho_test_n11;
show grant group hive_test_group1 on table src_autho_test_n11(key);

--column grant to group

grant select(key) on table src_autho_test_n11 to group hive_test_group1;

show grant group hive_test_group1 on table src_autho_test_n11;
show grant group hive_test_group1 on table src_autho_test_n11(key);

select key from src_autho_test_n11 order by key limit 20;

revoke select(key) on table src_autho_test_n11 from group hive_test_group1;
show grant group hive_test_group1 on table src_autho_test_n11;
show grant group hive_test_group1 on table src_autho_test_n11(key);

--role
create role sRc_roLE;
grant role sRc_roLE to user hive_test_user;
show role grant user hive_test_user;

--column grant to role

grant select(key) on table src_autho_test_n11 to role sRc_roLE;

show grant role sRc_roLE on table src_autho_test_n11;
show grant role sRc_roLE on table src_autho_test_n11(key);

select key from src_autho_test_n11 order by key limit 20;

revoke select(key) on table src_autho_test_n11 from role sRc_roLE;

--table grant to role

grant select on table src_autho_test_n11 to role sRc_roLE;

select key from src_autho_test_n11 order by key limit 20;

show grant role sRc_roLE on table src_autho_test_n11;
show grant role sRc_roLE on table src_autho_test_n11(key);
revoke select on table src_autho_test_n11 from role sRc_roLE;

-- drop role
drop role sRc_roLE;

