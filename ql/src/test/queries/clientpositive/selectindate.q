drop table if exists datetest;
create table datetest(dValue date, iValue int);
insert into datetest values('2000-03-22', 1);
insert into datetest values('2001-03-22', 2);
insert into datetest values('2002-03-22', 3);
insert into datetest values('2003-03-22', 4);
SELECT * FROM datetest WHERE dValue IN ('2000-03-22','2001-03-22');
drop table datetest;

