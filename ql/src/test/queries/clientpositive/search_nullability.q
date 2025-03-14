
create table test(a int, b string);
insert into table test values (1, "x");
insert into table test values (2, "y");
insert into table test values (null, "z");
insert into table test values (4, null);
insert into table test values (null, null);

select * from test;

select * from test where a is not null or a <= 2;
select * from test where a is null or a <= 2;
select * from test where a between 2 and 5 and a is null;
select * from test where a between 2 and 5 and a is not null;
select * from test where a between 2 and 5 or a is null;
select * from test where a between 2 and 5 or a is not null;

select * from test where b is not null or b <= "y";
select * from test where b is null or b <= "y";
select * from test where b between "x" and "z" and b is null;
select * from test where b between "x" and "z" and b is not null;
select * from test where b between "x" and "z" or b is null;
select * from test where b between "x" and "z" or b is not null;

select * from test where a is null and b is null;
select * from test where a is null or b is null;
select * from test where a is null and b is not null;
select * from test where a is null or b is not null;
select * from test where a is not null and b is null;
select * from test where a is not null or b is null;
select * from test where a is not null and b is not null;
select * from test where a is not null or b is not null;

select a is null and b is null from test;
select a is null or b is null from test;
select a is null and b is not null from test;
select a is null or b is not null from test;
select a is not null and b is null from test;
select a is not null or b is null from test;
select a is not null and b is not null from test;
select a is not null or b is not null from test;
