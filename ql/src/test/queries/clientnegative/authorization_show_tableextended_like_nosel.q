--! qt:authorizer
set user.name=user1;

create table t_like_a1(i int);
create table t_likeza1(i int);

grant select on table t_like_a1 to user user2;

set user.name=user2;
-- '_' is a LIKE wildcard, so the pattern also matches t_likeza1, which user2 cannot select
show table extended like 't_like_a1';
