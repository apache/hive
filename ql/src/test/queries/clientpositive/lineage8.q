set hive.exec.post.hooks=org.apache.hadoop.hive.ql.hooks.LineageLogger;

create table table_1 (id1 int, id2 int);
create table table_2 (id1 int, id2 int);

create table table_3 as
select id1 from table_1 t1 where t1.id2 = 1
union all
select id1 from table_2 t1 where t1.id2 = 2;

create table table_4 as
select id1 from (select id1,id2 from table_1 t1 where t1.id1 = 3 ) t1 where t1.id2 = 1
union all
select id1 from table_2 t1 where t1.id2 = 2;

create table table_5 as
select t.id1 from
(select id1 from table_1 t1 where t1.id2 = 1) t
join table_2 t1 on t.id1 = t1.id2;
