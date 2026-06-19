set hive.mapred.mode=nonstrict;

create table foo (d string);

create table foo_p (d string) partitioned by (p string);

insert into foo values ("1");

insert into foo_p partition (p="a ") select foo.d from foo;

insert into foo_p partition (p="a") select foo.d from foo;

select * from foo_p where p="a ";

select * from foo_p where p="a";
