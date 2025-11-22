CREATE TABLE tab_default_cons(c0 boolean, c1 int DEFAULT -15);

insert into tab_default_cons values(true, default);
insert into tab_default_cons values(false, 10);

select * from tab_default_cons;
