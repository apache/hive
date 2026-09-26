--! qt:authorizer
set user.name=user1;

create table t_show_props(i int);

grant all on table t_show_props to user user2;
revoke select on table t_show_props from user user2;

set user.name=user2;
show tblproperties t_show_props;
