-- UDTFs are not allowed as check expression
create table tconstr(i int check (explode(array(2,3))));
