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


