--! qt:authorizer
set user.name=user1;

create table amvnsp_table (a int, b varchar(256), c decimal(10,2));

insert into amvnsp_table values (1, 'alfred', 10.30),(2, 'bob', 3.14),(2, 'bonnie', 172342.2),(3, 'calvin', 978.76),(3, 'charlie', 9.8);


set user.name=user2;

create materialized view amvnsp_mat_view disable rewrite as select a, c from amvnsp_table;
