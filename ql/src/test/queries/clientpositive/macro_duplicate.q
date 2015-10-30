drop table macro_testing;
CREATE TABLE macro_testing(a int, b int, c int);

insert into table macro_testing values (1,2,3);
insert into table macro_testing values (4,5,6);

create temporary macro math_square(x int) x*x;
create temporary macro math_add(x int) x+x;

select math_square(a), math_square(b),factorial(a), factorial(b), math_add(a), math_add(b),int(c) from macro_testing order by int(c);
