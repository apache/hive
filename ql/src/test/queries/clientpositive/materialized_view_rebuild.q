create table rmv_table (cint int, cstring1 string);

insert into rmv_table values(1, 'fred'), (10, 'wilma');

create materialized view rmv_mat_view as select cint, cstring1 from rmv_table where cint < 10;

select * from rmv_mat_view;

insert into rmv_table values(2, 'barney'), (11, 'betty');

alter materialized view rmv_mat_view rebuild;

select * from rmv_mat_view;
