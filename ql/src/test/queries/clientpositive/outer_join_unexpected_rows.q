
with
t_y as (select col1 as id,col2 as s from (VALUES(0,'a'),(1,'y')) as c),
t_xy as (select col1 as id,col2 as s from (VALUES(1,'x'),(1,'y')) as c) 
select * from t_xy l full outer join t_y r on (l.id=r.id and l.s='y');

create table t_y (id integer,s string);
create table t_xy (id integer,s string);

insert into t_y values(0,'a'),(1,'y'),(1,'x');
insert into t_xy values(1,'x'),(1,'y');

select * from t_xy l full outer join t_y r on (l.id=r.id and l.s='y');
set hive.auto.convert.join=true;
select * from t_xy l full outer join t_y r on (l.id=r.id and l.s='y');
set hive.cbo.enable=false;
select * from t_xy l full outer join t_y r on (l.id=r.id and l.s='y');
