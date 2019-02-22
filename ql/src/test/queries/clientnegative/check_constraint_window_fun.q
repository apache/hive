-- Window functions are not allowed as check expression
create table tconstr(i int check (lead(i, 1) > 3));
