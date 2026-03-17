CREATE TABLE tab_default_cons(c0 boolean, c1 int DEFAULT -15);
insert into tab_default_cons values(true, default);
insert into tab_default_cons values(false, 10);
select * from tab_default_cons;

CREATE TABLE tab_default_cons1(c0 boolean, c1 double DEFAULT -log2(16));
insert into tab_default_cons1 values(true, default);
insert into tab_default_cons1 values(false, 20);
select * from tab_default_cons1;

CREATE TABLE tab_default_cons2(c0 boolean, c1 int DEFAULT -cast(-trim(' 12 ') as int));
insert into tab_default_cons2 values(true, default);
insert into tab_default_cons2 values(false, 30);
select * from tab_default_cons2;

CREATE TABLE tab_default_cons3(c0 boolean, c1 int DEFAULT +25);
insert into tab_default_cons3 values(true, default);
insert into tab_default_cons3 values(false, 40);
select * from tab_default_cons3;

CREATE TABLE tab_default_cons4(c0 boolean, c1 double DEFAULT cast(121 as double));
insert into tab_default_cons4 values(false, default);
insert into tab_default_cons4 values(true, 50);
select * from tab_default_cons4;