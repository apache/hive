CREATE TABLE tbl1_n5(key int, value string) CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS;

insert into tbl1_n5(key, value)
values
(0, 'val_0'),
(2, 'val_2'),
(9, 'val_9');

explain
SELECT t1.key from
(SELECT  key , row_number() over(partition by key order by value desc) as rk from tbl1_n5) t1
join
( SELECT key,count(distinct value) as cp_count from tbl1_n5 group by key) t2
on t1.key = t2.key where rk = 1;

SELECT t1.key from
(SELECT  key , row_number() over(partition by key order by value desc) as rk from tbl1_n5) t1
join
( SELECT key,count(distinct value) as cp_count from tbl1_n5 group by key) t2
on t1.key = t2.key where rk = 1;