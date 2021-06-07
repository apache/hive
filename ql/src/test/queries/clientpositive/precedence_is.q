create table emp(name string, age int);

insert into emp(name, age) values ('a', 5), ('b', 15), ('c', 12);

explain ast
select emp.age > 10 is true, (emp.age > 10) is true, not emp.age > 10 is true, not emp.age > 10 is not true from emp;
select emp.age > 10 is true, (emp.age > 10) is true, not emp.age > 10 is true, not emp.age > 10 is not true from emp;