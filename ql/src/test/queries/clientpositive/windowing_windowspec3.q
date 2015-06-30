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
           stock decimal(10,2),
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

-- Support timestamp datatype. Value in seconds (90days = 90 * 24 * 3600 seconds)
select deptno, empno, hirets, salary,
    sum(salary) over (partition by deptno order by hirets range 7776000 preceding),
    sum(salary) over (partition by deptno order by hirets range between 7776000 preceding and 7776000 following),
    sum(salary) over (partition by deptno order by hirets range between 7776000 preceding and 864000 preceding),
    sum(salary) over (partition by deptno order by hirets range between 864000 following and 7776000 following),
    sum(salary) over (partition by deptno order by hirets range between 864000 following and unbounded following),
    sum(salary) over (partition by deptno order by hirets range between unbounded preceding and 864000 following)
from emp;

-- Support double datatype
select deptno, empno, bonus,
    avg(bonus) over (partition by deptno order by bonus range 200 preceding),
    avg(bonus) over (partition by deptno order by bonus range between 200 preceding and 200 following),
    avg(bonus) over (partition by deptno order by bonus range between 200 preceding and 100 preceding),
    avg(bonus) over (partition by deptno order by bonus range between 100 following and 200 following),
    avg(bonus) over (partition by deptno order by bonus range between 200 following and unbounded following),
    avg(bonus) over (partition by deptno order by bonus range between unbounded preceding and 200 following)
from emp;

-- Support Decimal datatype
select deptno, empno, stock, salary,
    avg(salary) over (partition by deptno order by stock range 200 preceding),
    avg(salary) over (partition by deptno order by stock range between 200 preceding and 200 following),
    avg(salary) over (partition by deptno order by stock range between 200 preceding and 100 preceding),
    avg(salary) over (partition by deptno order by stock range between 100 following and 200 following),
    avg(salary) over (partition by deptno order by stock range between 200 following and unbounded following),
    avg(salary) over (partition by deptno order by stock range between unbounded preceding and 200 following)
from emp;
