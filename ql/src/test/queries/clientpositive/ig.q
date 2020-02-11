
drop table if exists t;
drop table if exists s;

-- suppose that this table is an external table or something
-- which supports the pushdown of filter condition on the id column
create table s(id integer, cnt integer);

-- create an internal table and an offset table
create table t(id integer, cnt integer);
create table t_offset(offset integer);
insert into t_offset values(0);

-- pretend that data is added to s
insert into s values(1,1);

from (select * from s
join t_offset on id>offset) s1
insert into t select id,cnt
insert overwrite table t_offset select max(s1.id)
;

insert into s values(2,2);
