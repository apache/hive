create table rmvwv_basetable (a int, b varchar(256), c decimal(10,2));

insert into rmvwv_basetable values (1, 'alfred', 10.30),(2, 'bob', 3.14),(2, 'bonnie', 172342.2),(3, 'calvin', 978.76),(3, 'charlie', 9.8);

create materialized view rmvwv_mat_view disable rewrite as select a, b, c from rmvwv_basetable;

create or replace view rmvwv_mat_view as select a, c from rmvwv_basetable;

