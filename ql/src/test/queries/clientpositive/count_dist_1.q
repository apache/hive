create table alltypes (a int);

explain cbo
select count(distinct 0), count(distinct null) from alltypes;

select count(distinct 0), count(distinct null) from alltypes;

insert into  alltypes(a) values (1), (2), (4), (3);

select count(distinct 0), count(distinct null) from alltypes;
