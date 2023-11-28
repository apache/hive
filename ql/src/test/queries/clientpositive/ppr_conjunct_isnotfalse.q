create table test0831 (id string) partitioned by (cp string);
insert into test0831 values ('a', '2022-08-23'),('c', '2022-08-23'),('d', '2022-08-23');
insert into test0831 values ('a', '2022-08-24'),('b', '2022-08-24');

-- Test conjunct has partition and non-partition column references wrapped by is not true
explain cbo
select * from test0831 where (id='a' and cp='2022-08-23') is not true;
explain
select * from test0831 where (id='a' and cp='2022-08-23') is not true;
select * from test0831 where (id='a' and cp='2022-08-23') is not true;
