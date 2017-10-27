-- SORT_QUERY_RESULTS

create table at1 (c1 int, c2 varchar(1), c3 varchar(10));

insert into at1 values (1, 'a', 'one');
insert into at1 values (2, 'b', 'two');
insert into at1 values (3, 'c', 'three');
insert into at1 values (4, 'd', 'four');
insert into at1 values (5, 'e', 'five');

create view av1 as select c1, c2 from at1;
DESCRIBE FORMATTED av1;
select * from av1;

alter view av1 as select c2, c3 from at1;
DESCRIBE FORMATTED av1;
select * from av1;

