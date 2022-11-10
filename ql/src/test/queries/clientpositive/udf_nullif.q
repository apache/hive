DESCRIBE FUNCTION nullif;
DESC FUNCTION EXTENDED nullif;

explain select nullif(1,2);
explain select nullif(1.0,2.0);
explain select nullif('y','x');

select	nullif(1,1);
select	nullif(2,1);
select	nullif('','x');
select	nullif('x','x');
select	nullif('x','');
select	nullif(1.0,2.0);
select	nullif(date('2011-11-11'),date('2011-11-11'));
select	nullif(date('2011-11-11'),date('2011-11-22'));
select	nullif(1,null);

select	nullif(1.0,1);


set hive.fetch.task.conversion=more;

drop table if exists t0;
create table t0(a int,b int,c float,d double precision);
insert into t0 values(1,2,3.1,4.1);
select	nullif(a,b),
	nullif(b,c),
	nullif(c,d),
	nullif(d,a) from t0;

SELECT assert_true(NULLIF(array(1,2,3),array(1,2,3)) is null);
SELECT assert_true(NULLIF(array(1,2,3),array(3,2,1)) is not null);

SELECT assert_true(NULLIF(named_struct("c", 1),named_struct("c", 1)) is null);
SELECT assert_true(NULLIF(named_struct("c", 1),named_struct("c", 2)) is not null);

SELECT assert_true(NULLIF(map('a',1,'b',2),map('a',1,'b',2)) is null);
SELECT assert_true(NULLIF(map('a',1,'b',2),map('a',1,'b',3)) is not null);
