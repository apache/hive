set hive.auto.convert.anti.join=true;
create table alltypestiny(
id int,
int_col int,
bigint_col bigint,
bool_col boolean
);

insert into alltypestiny(id, int_col, bigint_col, bool_col) values
(1, 1, 10, true),
(2, 4, 5, false),
(3, 5, 15, true),
(10, 10, 30, false);

create table alltypesagg(
id int,
int_col int,
bool_col boolean
);

insert into alltypesagg(id, int_col, bool_col) values
(1, 1, true),
(2, 4, false),
(5, 6, true),
(null, null, false);

select tt1.id
     from alltypestiny tt1 left JOIN alltypesagg tt2
     on tt1.int_col = tt2.int_col;

select *
from alltypesagg t1
where t1.id not in
    (select tt1.id
     from alltypestiny tt1 inner JOIN alltypesagg tt2
     on tt1.int_col = t1.int_col);

explain cbo select *
from alltypesagg t1
where t1.id not in
    (select tt1.id
     from alltypestiny tt1 inner JOIN alltypesagg tt2
     on tt1.int_col = t1.int_col);

create table ta
(
    id int,
    ca varchar(5)
);
insert into ta values (1, 'a1'), (2, 'a2'), (null, 'a3');
create table tb
(
    id int,
    cb varchar(5)
);
insert into tb values (1, 'b1');
create table tc
(
    cc varchar(5)
);
insert into tc values ('c1'), ('c2');
select *
from ta
where exists
    (select 1
     from tb inner JOIN tc
     on ta.id = tb.id);

select *
from ta
where not exists
    (select 1
     from tb inner JOIN tc
     on ta.id = tb.id);
