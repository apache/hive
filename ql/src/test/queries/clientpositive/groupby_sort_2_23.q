set hive.mapred.mode=nonstrict;
set hive.map.aggr=true;
set hive.explain.user=false;

create table test_bucket(age int, name string, dept string) clustered by (age, name) sorted by (age asc, name asc) into 2 buckets stored as ORC;
insert into test_bucket values (1, 'user1', 'dept1'), ( 2, 'user2' , 'dept2');
insert into test_bucket values (1, 'user1', 'dept1'), ( 2, 'user2' , 'dept2');

explain vectorization detail select age, name, count(*) from test_bucket group by  age, name having count(*) > 1;
select age, name, count(*) from test_bucket group by  age, name having count(*) > 1;
