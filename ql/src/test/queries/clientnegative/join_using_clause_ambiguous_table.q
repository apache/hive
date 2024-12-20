create table test(
    a int
);

select * from test
join test using(a)
join test using(a);
