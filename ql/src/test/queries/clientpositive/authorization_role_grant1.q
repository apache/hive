-- role granting without role keyword
create role src_role2;
grant  src_role2 to user user2 ;
show role grant user user2;

-- revoke role without role keyword
revoke src_role2 from user user2;
show role grant user user2;

----------------------------------------
-- role granting without role keyword, with admin option (syntax check)
----------------------------------------

create role src_role_wadmin;
grant  src_role_wadmin to user user2 with admin option;
show role grant user user2;

-- revoke role without role keyword
revoke src_role_wadmin from user user2 with admin option;
show role grant user user2;
