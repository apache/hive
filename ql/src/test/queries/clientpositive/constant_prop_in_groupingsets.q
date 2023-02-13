drop table tb1;

create table tb1 (key string, value string);

insert into tb1 values("a", "b");

with mid1 as (
    select 'test_value' as test_field, * from tb1
)
select key, nvl(test_field, 'default_test_value')
from mid1 group by key, test_field
grouping sets(key, test_field, (key, test_field));