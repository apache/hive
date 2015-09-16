drop table emps;

create table emps (empno int, deptno int, empname string);

insert into table emps values (1,2,"11"),(1,2,"11"),(3,4,"33"),(1,3,"11"),(2,5,"22"),(2,5,"22");

select * from emps;

select * from emps where (int(empno+deptno/2), int(deptno/3)) in ((2,0),(3,2));

select * from emps where (int(empno+deptno/2), int(deptno/3)) not in ((2,0),(3,2));

select * from emps where (empno,deptno) in ((1,2),(3,2));

select * from emps where (empno,deptno) not in ((1,2),(3,2));

select * from emps where (empno,deptno) in ((1,2),(1,3));

select * from emps where (empno,deptno) not in ((1,2),(1,3));

explain
select * from emps where (empno+1,deptno) in ((1,2),(3,2));

explain 
select * from emps where (empno+1,deptno) not in ((1,2),(3,2));

select * from emps where empno in (1,2);

select * from emps where empno in (1,2) and deptno > 2;

select * from emps where (empno) in (1,2) and deptno > 2;

select * from emps where ((empno) in (1,2) and deptno > 2);

explain select * from emps where ((empno*2)|1,deptno) in ((empno+1,2),(empno+2,2));

select * from emps where ((empno*2)|1,deptno) in ((empno+1,2),(empno+2,2));

select (empno*2)|1,substr(empname,1,1) from emps;

select * from emps where ((empno*2)|1,substr(empname,1,1)) in ((empno+1,'2'),(empno+2,'2'));

select * from emps where ((empno*2)|1,substr(empname,1,1)) not in ((empno+1,'2'),(empno+2,'2'));

select * from emps where ((empno*2)|1,substr(empname,1,1)) in ((empno+1,'2'),(empno+3,'2'));

select * from emps where ((empno*2)|1,substr(empname,1,1)) not in ((empno+1,'2'),(empno+3,'2'));


select sum(empno), empname from emps where ((empno*2)|1,substr(empname,1,1)) in ((empno+1,'2'),(empno+3,'2'))
group by empname;

select * from emps where ((empno*2)|1,substr(empname,1,1)) in ((empno+1,'2'),(empno+3,'2'))
union
select * from emps where (empno,deptno) in ((1,2),(3,2));

drop view v;

create view v as 
select * from(
select * from emps where ((empno*2)|1,substr(empname,1,1)) in ((empno+1,'2'),(empno+3,'2'))
union
select * from emps where (empno,deptno) in ((1,2),(3,2)))subq order by empno desc;

select * from v;

select subq.e1 from 
(select (empno*2)|1 as e1, substr(empname,1,1) as n1 from emps)subq
join
(select empno as e2 from emps where ((empno*2)|1,substr(empname,1,1)) in ((empno+1,'2'),(empno+3,'2')))subq2
on e1=e2+1;
