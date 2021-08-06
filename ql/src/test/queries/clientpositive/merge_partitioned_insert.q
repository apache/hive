drop table u;
drop table t;

create table t(id integer, value string default 'def');
create table u(id integer);

insert into t values(1,'xx');
insert into t (id) values(2);
insert into u values(3);

merge into t using u on t.id=u.id when not matched then insert (id) values (u.id);


