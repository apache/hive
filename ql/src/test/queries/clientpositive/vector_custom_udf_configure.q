set hive.fetch.task.conversion=none;

create temporary function UDFHelloTest as 'org.apache.hadoop.hive.ql.exec.vector.UDFHelloTest';

create table testorc1(id int, name string) stored as orc;
insert into table testorc1 values(1, 'a1'), (2,'a2');
 
set hive.vectorized.execution.enabled=true;
explain
select id, UDFHelloTest(name) from testorc1;
select id, UDFHelloTest(name) from testorc1;