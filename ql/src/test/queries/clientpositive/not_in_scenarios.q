CREATE TABLE my_tbl (id int);
insert into my_tbl values (100),(200),(300);
select * from my_tbl where id not in ('ABC', 'DEF');
select * from my_tbl where id not in ('ABC', 'DEF', '123');
select * from my_tbl where id not in ('ABC', 'DEF', '100');
select * from my_tbl where id not in (100, 'ABC', 200);
select * from my_tbl where id is not null or id in ("ABC");
select * from my_tbl where id is not null and id in ("ABC");