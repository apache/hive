--! qt:dataset:src
set hive.mapred.mode=nonstrict;

select * from src where (key, value) in (('238','val_238'));

drop table emps_n7;

create table emps_n7 (empno int, deptno int, empname string);

insert into table emps_n7 values (1,2,"11"),(1,2,"11"),(3,4,"33"),(1,3,"11"),(2,5,"22"),(2,5,"22");

select * from emps_n7;

select * from emps_n7 where (int(empno+deptno/2), int(deptno/3)) in ((3,2));

select * from emps_n7 where (int(empno+deptno/2), int(deptno/3)) not in ((3,2));

select * from emps_n7 where (empno,deptno) in ((3,2));

select * from emps_n7 where (empno,deptno) not in ((3,2));

select * from emps_n7 where (empno,deptno) in ((1,3));

select * from emps_n7 where (empno,deptno) not in ((1,3));

explain
select * from emps_n7 where (empno+1,deptno) in ((3,2));

explain 
select * from emps_n7 where (empno+1,deptno) not in ((3,2));

explain select * from emps_n7 where ((empno*2)|1,deptno) in ((empno+2,2));

select * from emps_n7 where ((empno*2)|1,deptno) in ((empno+2,2));

select (empno*2)|1,substr(empname,1,1) from emps_n7;

select * from emps_n7 where ((empno*2)|1,substr(empname,1,1)) in ((empno+2,'2'));

select * from emps_n7 where ((empno*2)|1,substr(empname,1,1)) not in ((empno+2,'2'));

select * from emps_n7 where ((empno*2)|1,substr(empname,1,1)) in ((empno+3,'2'));

select * from emps_n7 where ((empno*2)|1,substr(empname,1,1)) not in ((empno+3,'2'));


select sum(empno), empname from emps_n7 where ((empno*2)|1,substr(empname,1,1)) in ((empno+3,'2'))
group by empname;

select * from emps_n7 where ((empno*2)|1,substr(empname,1,1)) in ((empno+3,'2'))
union
select * from emps_n7 where (empno,deptno) in ((3,2));

drop view v_n11;

create view v_n11 as 
select * from(
select * from emps_n7 where ((empno*2)|1,substr(empname,1,1)) in ((empno+3,'2'))
union
select * from emps_n7 where (empno,deptno) in ((3,2)))subq order by empno desc;

select * from v_n11;

select subq.e1 from 
(select (empno*2)|1 as e1, substr(empname,1,1) as n1 from emps_n7)subq
join
(select empno as e2 from emps_n7 where ((empno*2)|1,substr(empname,1,1)) in ((empno+3,'2')))subq2
on e1=e2+1;
