create table purchase_history (s string, product string, price double, time int);
insert into purchase_history values ('1', 'Belt', 20.00, 21);
insert into purchase_history values ('1', 'Socks', 3.50, 31);
insert into purchase_history values ('3', 'Belt', 20.00, 51);
insert into purchase_history values ('4', 'Shirt', 15.50, 59);

create table cart_history (s string, cart_id int, time int);
insert into cart_history values ('1', 1, 10);
insert into cart_history values ('1', 2, 20);
insert into cart_history values ('1', 3, 30);
insert into cart_history values ('1', 4, 40);
insert into cart_history values ('3', 5, 50);
insert into cart_history values ('4', 6, 60);

create table events (s string, st2 string, n int, time int);
insert into events values ('1', 'Bob', 1234, 20);
insert into events values ('1', 'Bob', 1234, 30);
insert into events values ('1', 'Bob', 1234, 25);
insert into events values ('2', 'Sam', 1234, 30);
insert into events values ('3', 'Jeff', 1234, 50);
insert into events values ('4', 'Ted', 1234, 60);

explain
select s
from (
  select last.*, action.st2, action.n
  from (
    select purchase.s, purchase.time, max (mevt.time) as last_stage_time
    from (select * from purchase_history) purchase
    join (select * from cart_history) mevt
    on purchase.s = mevt.s
    where purchase.time > mevt.time
    group by purchase.s, purchase.time
  ) last
  join (select * from events) action
  on last.s = action.s and last.last_stage_time = action.time
) list;

select s
from (
  select last.*, action.st2, action.n
  from (
    select purchase.s, purchase.time, max (mevt.time) as last_stage_time
    from (select * from purchase_history) purchase
    join (select * from cart_history) mevt
    on purchase.s = mevt.s
    where purchase.time > mevt.time
    group by purchase.s, purchase.time
  ) last
  join (select * from events) action
  on last.s = action.s and last.last_stage_time = action.time
) list;

explain
select *
from (
  select last.*, action.st2, action.n
  from (
    select purchase.s, purchase.time, max (mevt.time) as last_stage_time
    from (select * from purchase_history) purchase
    join (select * from cart_history) mevt
    on purchase.s = mevt.s
    where purchase.time > mevt.time
    group by purchase.s, purchase.time
  ) last
  join (select * from events) action
  on last.s = action.s and last.last_stage_time = action.time
) list;

select *
from (
  select last.*, action.st2, action.n
  from (
    select purchase.s, purchase.time, max (mevt.time) as last_stage_time
    from (select * from purchase_history) purchase
    join (select * from cart_history) mevt
    on purchase.s = mevt.s
    where purchase.time > mevt.time
    group by purchase.s, purchase.time
  ) last
  join (select * from events) action
  on last.s = action.s and last.last_stage_time = action.time
) list;
