
create table test(
    a int
);

select * from test t1
join test t2 using(a)
join test t3 using(b);
