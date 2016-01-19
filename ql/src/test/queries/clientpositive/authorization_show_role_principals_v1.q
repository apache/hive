set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;
create role role1;
grant role1 to user user1 with admin option;
grant role1 to user user2 with admin option;
show role grant user user1;
show role grant user user2;
show principals role1;
