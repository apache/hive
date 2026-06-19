set hive.auto.convert.anti.join=true;
create table alltypestiny(
id int,
int_col int,
bigint_col bigint,
bool_col boolean
);

insert into alltypestiny(id, int_col, bigint_col, bool_col) values
(1, 1, 10, true),
(2, 4, 5, false),
(3, 5, 15, true),
(10, 10, 30, false);

create table alltypesagg(
id int,
int_col int,
bool_col boolean
);

insert into alltypesagg(id, int_col, bool_col) values
(1, 1, true),
(2, 4, false);


explain cbo
select id, int_col
from alltypesagg a
where exists
  (select sum(int_col) over (partition by bool_col)
   from alltypestiny b
   where a.id = b.id);

explain
select id, int_col
from alltypesagg a
where exists
  (select sum(int_col) over (partition by bool_col)
   from alltypestiny b
   where a.id = b.id);

select id, int_col
from alltypesagg a
where exists
  (select sum(int_col) over (partition by bool_col)
   from alltypestiny b
   where a.id = b.id);



explain cbo
select id, int_col from alltypestiny t
where not exists
  (select sum(int_col) over (partition by bool_col)
   from alltypesagg a where t.id = a.int_col);

explain
select id, int_col from alltypestiny t
where not exists
  (select sum(int_col) over (partition by bool_col)
   from alltypesagg a where t.id = a.int_col);

select id, int_col from alltypestiny t
where not exists
  (select sum(int_col) over (partition by bool_col)
   from alltypesagg a where t.id = a.int_col);
