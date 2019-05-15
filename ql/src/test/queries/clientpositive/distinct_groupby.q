--! qt:dataset:src
--! qt:dataset:src1

explain select distinct key from src1 group by key,value;
select distinct key from src1 group by key,value;

explain select distinct count(value) from src group by key;
select distinct count(value) from src group by key;

explain select distinct count(*) from src1 where key in (128,146,150);
select distinct count(*) from src1 where key in (128,146,150);

explain select distinct * from (select distinct count(*) from src1 where key in (128,146,150)) as T;
select distinct * from (select distinct count(*) from src1 where key in (128,146,150)) as T;

explain select distinct count(*)+1 from src1;
select distinct count(*)+1 from src1;

explain select distinct count(a.value), count(b.value) from src a join src1 b on a.key=b.key;
select distinct count(a.value), count(b.value) from src a join src1 b on a.key=b.key;

explain select distinct c from (select distinct key, count(*) as c from src1 where key in (128,146,150) group by key) a;
select distinct c from (select distinct key, count(*) as c from src1 where key in (128,146,150) group by key) a;

explain select distinct key from src1;
select distinct key from src1;

explain select distinct * from src1;
select distinct * from src1;

explain select distinct count(*) from src1 where key in (128,146,150) group by key;
select distinct count(*) from src1 where key in (128,146,150) group by key;

explain select distinct key, count(*) from src1 where key in (128,146,150) group by key;
select distinct key, count(*) from src1 where key in (128,146,150) group by key;

explain select distinct * from (select * from src1) as T;
select distinct * from (select * from src1) as T;

explain select distinct * from (select count(*) from src1) as T;
select distinct * from (select count(*) from src1) as T;

explain select distinct * from (select * from src1 where key in (128,146,150)) as T;
select distinct * from (select * from src1 where key in (128,146,150)) as T;

explain select distinct key from (select * from src1 where key in (128,146,150)) as T;
select distinct key from (select * from src1 where key in (128,146,150)) as T;

explain select distinct * from (select count(*) from src1 where key in (128,146,150)) as T;
select distinct * from (select count(*) from src1 where key in (128,146,150)) as T;

explain select distinct sum(key) over () from src1;
select distinct sum(key) over () from src1;

explain select distinct * from (select sum(key) over () from src1) as T;
select distinct * from (select sum(key) over () from src1) as T;

explain select distinct value, key, count(1) over (partition by value) from src1;
select distinct value, key, count(1) over (partition by value) from src1;

explain select value, key, count(1) over (partition by value) from src1 group by value, key;
select value, key, count(1) over (partition by value) from src1 group by value, key;

explain select value, key, count(1) over (partition by value) from src1;
select value, key, count(1) over (partition by value) from src1;

explain select distinct count(*)+key from src1 group by key;
select distinct count(*)+key from src1 group by key;

explain select distinct count(a.value), count(b.value) from src a join src1 b on a.key=b.key group by a.key;
select distinct count(a.value), count(b.value) from src a join src1 b on a.key=b.key group by a.key;

-- should not project the virtual BLOCK_OFFSET et all columns
explain select distinct * from (select distinct * from src1) as T;
select distinct * from (select distinct * from src1) as T;

