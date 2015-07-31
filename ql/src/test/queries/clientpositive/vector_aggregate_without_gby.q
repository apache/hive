create table testvec(id int, dt int, greg_dt string) stored as orc;
insert into table testvec
values 
(1,20150330, '2015-03-30'),
(2,20150301, '2015-03-01'),
(3,20150502, '2015-05-02'),
(4,20150401, '2015-04-01'),
(5,20150313, '2015-03-13'),
(6,20150314, '2015-03-14'),
(7,20150404, '2015-04-04');
set hive.vectorized.execution.enabled=true;
set hive.map.aggr=true;
explain select max(dt), max(greg_dt) from testvec where id=5;
select max(dt), max(greg_dt) from testvec where id=5;