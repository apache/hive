-- Test cases with subqueries having complex correlation predicates. 

set hive.auto.convert.anti.join=true;

-- HIVE-24957: Wrong results when subquery has COALESCE in correlation predicate
create table author
(
    a_authorkey int,
    a_name      varchar(50)
);

create table book
(
    b_bookkey   int,
    b_title     varchar(50),
    b_authorkey int
);

insert into author
values (10, 'Victor Hugo');
insert into author
values (20, 'Alexandre Dumas');
insert into author
values (300, 'UNKNOWN1');
insert into author
values (null, 'UNKNOWN2');

insert into book
values (1, 'Les Miserables', 10);
insert into book
values (2, 'The Count of Monte Cristo', 20);
insert into book
values (3, 'Men Without Women', 30);
insert into book
values (4, 'Odyssey', null);

explain cbo
select b.b_title
from book b
where exists
          (select a_authorkey
           from author a
           where coalesce(b.b_authorkey, 300) = a.a_authorkey);

select b.b_title
from book b
where exists
          (select a_authorkey
           from author a
           where coalesce(b.b_authorkey, 300) = a.a_authorkey);

explain cbo
select b.b_title
from book b
where exists
          (select a_authorkey
           from author a
           where coalesce(b.b_authorkey, 400) = coalesce(a.a_authorkey, 400));

select b.b_title
from book b
where exists
          (select a_authorkey
           from author a
           where coalesce(b.b_authorkey, 400) = coalesce(a.a_authorkey, 400));

explain cbo
select b.b_title
from book b
where not exists
          (select a_authorkey
           from author a
           where coalesce(b.b_authorkey, 400) = coalesce(a.a_authorkey, 400));

select b.b_title
from book b
where not exists
          (select a_authorkey
           from author a
           where coalesce(b.b_authorkey, 400) = coalesce(a.a_authorkey, 400));
