drop table if exists binary_cast_insert;

create table binary_cast_insert (b binary);

insert into table binary_cast_insert values (cast("a" as binary));
insert into table binary_cast_insert values (cast("hello" as binary));
insert into table binary_cast_insert values (cast(cast("hello" as char(10)) as binary));
insert into table binary_cast_insert values (cast(cast("hello" as varchar(10)) as binary));

select * from binary_cast_insert;
