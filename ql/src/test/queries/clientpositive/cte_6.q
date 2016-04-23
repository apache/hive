explain
with Q1 as ( select key from sRc where key = '5')
select CPS.key from Q1 CPS;

-- chaining

explain
with Q1 as ( select key from q2 where key = '5'),
Q2 as ( select key from sRc where key = '5')
select CPS.key from Q1 CPS;
