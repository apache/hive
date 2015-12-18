set hive.security.authorization.createtable.owner.grants=ALL;

create table default_auth_table_creator_priv_test(i int);

-- Table owner (hive_test_user) should have ALL privileges
show grant on table default_auth_table_creator_priv_test;
