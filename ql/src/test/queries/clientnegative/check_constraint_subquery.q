-- Subqueries are not allowed as check expression
create table tconstr(i int check (3=(select count(*) from t)));
