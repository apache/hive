explain extended
select * from srcpart a where a.ds='2008-04-08';

select * from srcpart a where a.ds='2008-04-08';


explain extended
select * from srcpart a where a.ds='2008-04-08' and key < 200;

select * from srcpart a where a.ds='2008-04-08' and key < 200;


explain extended
select * from srcpart a where a.ds='2008-04-08' and rand(100) < 0.1;

select * from srcpart a where a.ds='2008-04-08' and rand(100) < 0.1;
