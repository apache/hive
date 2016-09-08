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
