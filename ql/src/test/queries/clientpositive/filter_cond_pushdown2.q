set hive.cbo.enable=false;

drop table if exists users_table;
CREATE TABLE users_table(
  `field_1` int,
  `field_2` string,
  `field_3` boolean,
  `field_4` boolean,
  `field_5` boolean,
  `field_6` boolean,
  `field_7` boolean)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;
load data local inpath '../../data/files/small_csv.csv' into table users_table;

explain
with all_hits as (
select * from users_table
),
all_exposed_users as (
select distinct
field_1,
field_2
from all_hits
where field_3
),
interacted as (
select distinct
field_1,
field_2
from all_hits
where field_4
)
select
all_exposed_users.field_1,
count(*) as nr_exposed,
sum(if(interacted.field_2 is not null, 1, 0)) as nr_interacted
from all_exposed_users
left outer join interacted
on all_exposed_users.field_1 = interacted.field_1
and all_exposed_users.field_2 = interacted.field_2
group by all_exposed_users.field_1
order by all_exposed_users.field_1;

with all_hits as (
select * from users_table
),
all_exposed_users as (
select distinct
field_1,
field_2
from all_hits
where field_3
),
interacted as (
select distinct
field_1,
field_2
from all_hits
where field_4
)
select
all_exposed_users.field_1,
count(*) as nr_exposed,
sum(if(interacted.field_2 is not null, 1, 0)) as nr_interacted
from all_exposed_users
left outer join interacted
on all_exposed_users.field_1 = interacted.field_1
and all_exposed_users.field_2 = interacted.field_2
group by all_exposed_users.field_1
order by all_exposed_users.field_1;
