DESCRIBE FUNCTION space;
DESCRIBE FUNCTION EXTENDED space;

create table udf_space(i int);
insert into udf_space values (5),(6),(7),(8),(9),(10),(11),(12),(13),(14);
select i, SPACE(i) from udf_space;
