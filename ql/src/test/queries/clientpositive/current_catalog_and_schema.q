use default;

select current_catalog();
select current_schema();

create database test_current_database;
use test_current_database;

select current_catalog();
select current_schema();