create table empsalary (depname varchar(10), salary int);

SELECT sum(salary) OVER w as s , avg(salary) OVER w as a
  FROM empsalary
  order by s
  WINDOW w AS (PARTITION BY depname ORDER BY salary DESC);

