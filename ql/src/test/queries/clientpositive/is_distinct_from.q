--! qt:dataset:part
explain select 1 is distinct from 1,
               1 is distinct from 2,
               1 is distinct from null,
               null is distinct from null
         from part;

select 1 is distinct from 1,
               1 is distinct from 2,
               1 is distinct from null,
               null is distinct from null
         from part;

explain select 1 is not distinct from 1,
               1 is not distinct from 2,
               1 is not distinct from null,
               null is not distinct from null
         from part;

select 1 is not distinct from 1,
               1 is not distinct from 2,
               1 is not distinct from null,
               null is not distinct from null
         from part;

create table test_n5(x string, y string);
insert into test_n5 values ('q', 'q'), ('q', 'w'), (NULL, 'q'), ('q', NULL), (NULL, NULL);
select *, x is not distinct from y, not (x is not distinct from y), (x is distinct from y) = true from test_n5;

select *, x||y is not distinct from y||x, not (x||y||x is not distinct from y||x||x) from test_n5;

-- where
explain select * from test_n5 where y is distinct from null;
select * from test_n5 where y is distinct from null;

explain select * from test_n5 where y is not distinct from null;
select * from test_n5 where y is not distinct from null;
drop table test_n5;

-- where
explain select * from part where p_size is distinct from 2;
select * from part where p_size is distinct from 2;

explain select * from part where p_size is not distinct from 2;
select * from part where p_size is not distinct from 2;


-- insert
create table t2(c0 boolean, c1 float );
insert into t2(c0) values (not (0.379 is not distinct from 641));
insert into t2(c0,c1) values (not (0.379 is not distinct from 641), 0.2);

select * from t2;


create table if not exists t0(c0 boolean unique disable novalidate, c1 float, c2 boolean);
insert into t0(c2, c1, c0)
values (0.4144825 is distinct from 0.6828972,
        0.14, true);

select * from t0;
