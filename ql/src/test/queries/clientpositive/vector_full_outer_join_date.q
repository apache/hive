set hive.vectorized.execution.enabled=true;
set hive.optimize.dynamic.partition.hashjoin=true;
set hive.auto.convert.join=true;

create table tbl1 (id int, event_date date);
create table tbl2 (id int, event_date date);

insert into tbl1 values (1, '2023-01-01'), (2, '2023-01-02'), (3, '2023-01-03');
insert into tbl2 values (2, '2023-01-02'), (3, '2023-01-04'), (4, '2023-01-05');

explain vectorization detail select * from tbl1 full outer join tbl2 on tbl1.event_date = tbl2.event_date order by tbl1.id, tbl2.id;

select * from tbl1 full outer join tbl2 on tbl1.event_date = tbl2.event_date order by tbl1.id, tbl2.id;

