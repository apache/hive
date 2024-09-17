set hive.fetch.task.conversion=none;

create table t_n14(a int, b int);

insert into t_n14 values (1,2),(1,2),(1,3),(2,4),(20,-100),(-1000,100),(4,5),(3,7),(8,9);

select a as b, b as a from t_n14 order by a, b;
select a as b, b as a from t_n14 order by t_n14.a, t_n14.b;
select a as b from t_n14 order by b;
select a as b from t_n14 order by 0-a;
select a,b,count(*),a+b from t_n14 group by a, b order by a+b, a;

create table store(store_name string, store_sqft int);

insert into store values ('HQ', 3), ('hq', 4);

-- Order by expression has string literal
-- literal is upper case both in project and order by clause
explain cbo
select store.store_name as c0, case store_name when 'HQ' then null else store_name end as c1,
       store.store_sqft as c2
  from store as store
group by store.store_name, case store_name when 'HQ' then null else store_name end, store.store_sqft
order by case store_name when 'HQ' then null else store_name end ASC;

select store.store_name as c0, case store_name when 'HQ' then null else store_name end as c1,
       store.store_sqft as c2
  from store as store
group by store.store_name, case store_name when 'HQ' then null else store_name end, store.store_sqft
order by case store_name when 'HQ' then null else store_name end ASC;

-- literal has different case in project and order by clause
explain cbo
select store.store_name as c0, case store_name when 'HQ' then null else store_name end as c1,
       store.store_sqft as c2
  from store as store
group by store.store_name, case store_name when 'HQ' then null else store_name end, store.store_sqft
order by case store_name when 'hq' then null else store_name end ASC;

select store.store_name as c0, case store_name when 'HQ' then null else store_name end as c1,
       store.store_sqft as c2
  from store as store
group by store.store_name, case store_name when 'HQ' then null else store_name end, store.store_sqft
order by case store_name when 'hq' then null else store_name end ASC;

-- expression is referenced by alias in Order by clause
explain cbo
select store.store_name as c0, case store_name when 'HQ' then null else store_name end as c1,
       store.store_sqft as c2
  from store as store
group by store.store_name, case store_name when 'HQ' then null else store_name end, store.store_sqft
order by c1 ASC;

select store.store_name as c0, case store_name when 'HQ' then null else store_name end as c1,
       store.store_sqft as c2
  from store as store
group by store.store_name, case store_name when 'HQ' then null else store_name end, store.store_sqft
order by c1 ASC;
