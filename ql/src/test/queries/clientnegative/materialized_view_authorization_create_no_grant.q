--! qt:authorizer
set user.name=user1;

create table amvcng_gtable (a int, b varchar(256), c decimal(10,2));

insert into amvcng_gtable values (1, 'alfred', 10.30),(2, 'bob', 3.14),(2, 'bonnie', 172342.2),(3, 'calvin', 978.76),(3, 'charlie', 9.8);

grant select on table amvcng_gtable to user user2;

set user.name=user2;

create materialized view amvcng_gmat_view disable rewrite as select a, c from amvcng_gtable;
