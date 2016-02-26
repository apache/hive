-- SORT_QUERY_RESULTS

explain
select * from src a join src1 b on a.key = b.key;

select * from src a join src1 b on a.key = b.key;

explain
select * from
(select * from src where 1 = 0)a
join
(select * from src1)b on a.key = b.key;

select * from
(select * from src where 1 = 0)a
join
(select * from src1)b on a.key = b.key;

explain
select * from
(select * from src where 1 = 0)a
left outer join
(select * from src1)b on a.key = b.key;

select * from
(select * from src where 1 = 0)a
left outer join
(select * from src1)b on a.key = b.key;

explain
select * from
(select * from src where 1 = 0)a
right outer join
(select * from src1)b on a.key = b.key;

select * from
(select * from src where 1 = 0)a
right outer join
(select * from src1)b on a.key = b.key;

explain
select * from
(select * from src where 1 = 0)a
full outer join
(select * from src1)b on a.key = b.key;

select * from
(select * from src where 1 = 0)a
full outer join
(select * from src1)b on a.key = b.key;

explain
select * from
(select * from src)a
right outer join
(select * from src1 where 1 = 0)b on a.key = b.key;

select * from
(select * from src)a
right outer join
(select * from src1 where 1 = 0)b on a.key = b.key;
