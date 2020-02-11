-- FULLY Qualified names are not allowed as check expression
create table tconstr(i int, j int CHECK (tconstr.i>j));
