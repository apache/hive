--! qt:transactional

create table u(id integer);
insert into u values(3);

create table t1(id integer, value string default 'def');
insert into t1 (id,value) values(1,'xx');
insert into t1 (id) values(2);

merge into t1 t using u on t.id=u.id when not matched then insert (id) values (u.id);

select id,value from t1 order by id;

create table t2(value string default 'def') partitioned by (id integer);
insert into t2 (id,value) values(1,'xx');
insert into t2 (id) values(2);

merge into t2 t using u on t.id=u.id when not matched then insert (id) values (u.id);

select id,value from t2 order by id;
