create table store(store_name string, store_sqft int);

insert into store values ('f', 10), ('a', 42), ('a', 12);

-- order by expression having upper case string literal
select 'HQ' || store_name as c1
from store as store
order by 'HQ' || store_name;

select distinct 'HQ' || store_name as c1
from store as store
order by 'HQ' || store_name;

select distinct 'HQ' || store_name as c1
from store as store
group by 'HQ' || store_name
order by 'HQ' || store_name;

select 'HQ' || store_name as c1
from store as store
group by 'HQ' || store_name, store_sqft
order by 'HQ' || store_name;

select distinct 'HQ' || store_name as c1
from store as store
group by 'HQ' || store_name, store_sqft
order by 'HQ' || store_name;