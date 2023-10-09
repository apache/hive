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

-- Metadata delete should not be done here and fallback to normal delete.
create table ice_str_name (first_name string, last_name string) stored by iceberg stored as orc tblproperties ('format-version'='2');
insert into table ice_str_name values ('Alex', 'Clark');
insert into table ice_str_name values ('Bob', 'Bob');

delete from ice_str_name where first_name = last_name;

select * from ice_str_name;

-- Metadata delete should not be done here and fallback to normal delete.
create table ice_int_id (first_id int, last_id int) stored by iceberg stored as orc tblproperties ('format-version'='2');
insert into table ice_int_id values (7, 9);
insert into table ice_int_id values (8, 8);

delete from ice_int_id where first_id = last_id;

select * from ice_int_id;

drop table ice_date;
drop table ice_date_year;
drop table ice_str_name;
drop table ice_int_id;