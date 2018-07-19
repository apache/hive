set hive.explain.user=true;

create table d1(d date);
--  tblproperties('transactional'='false');

insert into d1 values
	('2010-10-01'),
	('2010-10-02'),
	('2010-10-03'),
	('2010-10-04'),
	('2010-10-05'),
	('2010-10-06'),
	('2010-10-07'),
	('2010-10-08'),
	('2010-10-09'),
	('2010-10-10');

analyze table d1 compute statistics for columns;

desc formatted d1;
desc formatted d1 d;

explain
select 'stats: FIL ~0 read',count(1) from d1 where d < '2010-03-01';

explain
select 'stats: FIL estimate some read',count(1) from d1 where d < '2010-10-03';

explain
select 'stats: FIL estimate all read',count(1) from d1 where d < '2010-11-03';
