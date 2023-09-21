create table ice_date (a int, b date) stored by iceberg stored as orc tblproperties ('format-version'='2');

insert into table ice_date values (1, '2021-01-01');
insert into table ice_date values (2, '2022-02-02'), (3, '2022-03-03');

delete from ice_date where b = '2022-02-02';
delete from ice_date where a = 1 and b = '2021-01-01';

select * from ice_date;

create table ice_date_year (a int, b date) stored by iceberg stored as orc tblproperties ('format-version'='2');
insert into table ice_date_year values (1, '2021-01-01');
insert into table ice_date_year values (2, '2022-02-02'), (3, '2022-03-03');

delete from ice_date_year where year(b) = 2022;

select * from ice_date_year;