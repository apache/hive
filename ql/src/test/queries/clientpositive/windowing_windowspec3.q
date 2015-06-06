-- Test value based windowing spec

drop table if exists emp;

create table emp(empno smallint,
           ename varchar(10),
           job varchar(10),
           manager smallint,
           hiredate date,
           hirets timestamp,
           salary double,
           bonus double,
           deptno tinyint)
       row format delimited
       fields terminated by '|';

load data local inpath '../../data/files/emp2.txt' into table emp;

-- No order by
select hirets, salary, sum(salary) over (partition by hirets range between current row and unbounded following) from emp;


-- Support date datatype
select deptno, empno, hiredate, salary,
    sum(salary) over (partition by deptno order by hiredate range 90 preceding),
    sum(salary) over (partition by deptno order by hiredate range between 90 preceding and 90 following),
    sum(salary) over (partition by deptno order by hiredate range between 90 preceding and 10 preceding),
    sum(salary) over (partition by deptno order by hiredate range between 10 following and 90 following),
    sum(salary) over (partition by deptno order by hiredate range between 10 following and unbounded following),
    sum(salary) over (partition by deptno order by hiredate range between unbounded preceding and 10 following)
from emp;
