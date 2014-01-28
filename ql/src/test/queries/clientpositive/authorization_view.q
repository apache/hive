-- SORT_BEFORE_DIFF

create view src_autho_test as select * from src;

set hive.security.authorization.enabled=true;

--view grant to user

grant select on view src_autho_test to user hive_test_user;

show grant user hive_test_user on view src_autho_test;
show grant user hive_test_user on view src_autho_test(key);

revoke select on view src_autho_test from user hive_test_user;
show grant user hive_test_user on view src_autho_test;
show grant user hive_test_user on view src_autho_test(key);

--column grant to user

grant select(key) on view src_autho_test to user hive_test_user;

show grant user hive_test_user on view src_autho_test;
show grant user hive_test_user on view src_autho_test(key);

revoke select(key) on view src_autho_test from user hive_test_user;
show grant user hive_test_user on view src_autho_test;
show grant user hive_test_user on view src_autho_test(key); 

--view grant to group

grant select on view src_autho_test to group hive_test_group1;

show grant group hive_test_group1 on view src_autho_test;
show grant group hive_test_group1 on view src_autho_test(key);

revoke select on view src_autho_test from group hive_test_group1;
show grant group hive_test_group1 on view src_autho_test;
show grant group hive_test_group1 on view src_autho_test(key);

--column grant to group

grant select(key) on view src_autho_test to group hive_test_group1;

show grant group hive_test_group1 on view src_autho_test;
show grant group hive_test_group1 on view src_autho_test(key);

revoke select(key) on view src_autho_test from group hive_test_group1;
show grant group hive_test_group1 on view src_autho_test;
show grant group hive_test_group1 on view src_autho_test(key);

--role
create role src_role;
grant role src_role to user hive_test_user;
show role grant user hive_test_user;

--column grant to role

grant select(key) on view src_autho_test to role src_role;

show grant role src_role on view src_autho_test;
show grant role src_role on view src_autho_test(key);

revoke select(key) on view src_autho_test from role src_role;

--view grant to role

grant select on view src_autho_test to role src_role;

show grant role src_role on view src_autho_test;
show grant role src_role on view src_autho_test(key);
revoke select on view src_autho_test from role src_role;

-- drop role
drop role src_role;

set hive.security.authorization.enabled=false;
drop view src_autho_test;
