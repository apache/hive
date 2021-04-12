--! qt:dataset:part
--! qt:dataset:src

-- HIVE-24999: HiveSubQueryRemoveRule generates invalid plan for IN subquery with correlations
-- Without the fix queries below fail with AssertionError at compilation time when -Dcalcite.debug is enabled. After
-- Calcite 1.27.0 the queries would fail irrespective of the value of -Dcalcite.debug property.

explain cbo
select *
from src b
where b.key in (select a.key from src a where b.value = a.value and a.key > '9');

explain cbo
select count(*) as c
from part as e
where p_size + 100 IN (select p_partkey from part where p_name = e.p_name);

explain cbo
select *
from part
where p_name IN (select p_name from part p where p.p_size = part.p_size AND part.p_size + 121150 = p.p_partkey);

explain cbo
select *
from src b
where b.key in (select key from src a where b.value > a.value and b.key < a.key);

explain cbo select p_mfgr, p_name, p_size
from part b where b.p_size in
                  (select min(p_size)
                   from (select p_mfgr, p_size, rank() over(partition by p_mfgr order by p_size) as r from part) a
                   where r <= 2 and b.p_mfgr = a.p_mfgr
                  );

select p_mfgr, p_name, p_size
from part b where b.p_size in
                  (select min(p_size)
                   from (select p_mfgr, p_size, rank() over(partition by p_mfgr order by p_size) as r from part) a
                   where r <= 2 and b.p_mfgr = a.p_mfgr
                  );

explain cbo select *
            from src b
            where b.key in
                  (select distinct a.key
                   from src a
                   where b.value <> a.key and a.key > '9'
                  );

select *
from src b
where b.key in
      (select distinct a.key
       from src a
       where b.value <> a.key and a.key > '9'
      );