--! qt:authorizer
set user.name=user1;

create table t_show_ext(i int);

grant all on table t_show_ext to user user2;
revoke select on table t_show_ext from user user2;

set user.name=user2;
show table extended like 't_show_ext';
