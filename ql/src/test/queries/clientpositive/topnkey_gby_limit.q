SET hive.optimize.topnkey=false;

set tez.grouping.max-size=50;
set tez.grouping.min-size=25;

create table test(id string);

insert into test(id) values
(4), (3), (4), (3), (4), (3), (4), (3), (4), (3), (4), (3), (4), (3), (4), (3), (4), (3), (4), (2), (1), (5);

explain cbo
select id, count(1) from test group by id limit 2;
explain extended
select id, count(1) from test group by id limit 2;
select id, count(1) from test group by id limit 2;

SET hive.optimize.topnkey=true;

explain extended
select id, count(1) from test group by id limit 2;
select id, count(1) from test group by id limit 2;
