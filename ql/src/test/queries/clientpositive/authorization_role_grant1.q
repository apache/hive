set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory;
-- enable sql standard authorization

-- role granting without role keyword
create role src_role2;
grant  src_role2 to user user2 ;
show role grant user user2;
show roles;

-- revoke role without role keyword
revoke src_role2 from user user2;
show role grant user user2;
show roles;

----------------------------------------
-- role granting without role keyword, with admin option (syntax check)
----------------------------------------

create role src_role_wadmin;
grant  src_role_wadmin to user user2 with admin option;
show role grant user user2;

-- revoke role without role keyword
revoke src_role_wadmin from user user2 with admin option;
show role grant user user2;



-- drop roles
show roles;
drop role src_role2;
show roles;
drop role src_role_wadmin;
show roles;
