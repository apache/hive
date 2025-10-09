
create table test (
 a int
);

insert into test values (1);

create table test1 (
 a int,
 b int
);

insert into test1 values (1, 2);

create table test2 (
 a int,
 b int,
 c int
);

insert into test2 values (1, 2, 3);

-- self join with 1 column
select * from test t1
join test t2 using(a)
join test t3 using(a);

explain cbo select * from test t1
join test t2 using(a)
join test t3 using(a);

-- self join with multiple columns
select * from test1 t1
join test1 t2 using(a)
join test1 t3 using(b);

explain cbo select * from test1 t1
join test1 t2 using(a)
join test1 t3 using(b);

-- joins with multiple tables and columns
select * from test1 t1
join test t2 using(a)
join test2 t3 using(b);

explain cbo select * from test1 t1
join test t2 using(a)
join test2 t3 using(b);

select * from test t1
join test1 t2 using(a)
join test2 t3 using(b)
join test2 t4 using(c);

explain cbo select * from test t1
join test1 t2 using(a)
join test2 t3 using(b)
join test2 t4 using(c);

select * from test1 t1
join test2 t2 using(a, b)
join test2 t3 using(c, b)
join test t4 using(a);

explain cbo select * from test1 t1
join test2 t2 using(a, b)
join test2 t3 using(c, b)
join test t4 using(a);

-- joins without table alias
select * from test1
join test using(a)
join test2 using(b);

explain cbo select * from test1
join test using(a)
join test2 using(b);
