set hive.optimize.dynamic.partition.hashjoin=true;
set hive.auto.convert.join=true;

-- Test Date column
create table tbl1 (id int, event_date date);
create table tbl2 (id int, event_date date);

insert into tbl1 values (1, '2023-01-01'), (2, '2023-01-02'), (3, '2023-01-03');
insert into tbl2 values (2, '2023-01-02'), (3, '2023-01-04'), (4, '2023-01-05');

select tbl1.id, tbl1.event_date from tbl1 full outer join tbl2 on tbl1.event_date = tbl2.event_date order by tbl1.id;

-- Test timestamp column
create table tbl3 (id int, event_date timestamp);
create table tbl4 (id int, event_date timestamp);

insert into tbl3 values (1, '2025-12-17 10:20:30'), (2, '2025-12-17 11:20:30');
insert into tbl4 values (2, '2025-12-17 11:20:30'), (3, '2025-12-17 09:20:30');

select tbl3.id, tbl3.event_date from tbl3 full outer join tbl4 on tbl3.event_date = tbl4.event_date order by tbl3.id;

-- Test Double column
create table tbl5 (id int, val double);
create table tbl6 (id int, val double);

insert into tbl5 values (1, 5.6D), (2, 3.2D);
insert into tbl6 values (2, 3.2D), (3, 7.2D);

select tbl5.id, tbl5.val from tbl5 full outer join tbl6 on tbl5.val = tbl6.val order by tbl5.id;
