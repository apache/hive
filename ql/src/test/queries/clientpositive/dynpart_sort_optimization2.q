set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.optimize.sort.dynamic.partition=true;
set hive.exec.dynamic.partition=true;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=1000;
set hive.exec.dynamic.partition.mode=nonstrict;



-- SORT_QUERY_RESULTS

drop table ss;
drop table ss_orc;
drop table ss_part;
drop table ss_part_orc;

create table ss (
ss_sold_date_sk int,
ss_net_paid_inc_tax float,
ss_net_profit float);

create table ss_part (
ss_net_paid_inc_tax float,
ss_net_profit float)
partitioned by (ss_sold_date_sk int);

load data local inpath '../../data/files/dynpart_test.txt' overwrite into table ss;

explain insert overwrite table ss_part partition (ss_sold_date_sk)
select ss_net_paid_inc_tax,
  ss_net_profit,
  ss_sold_date_sk
  from ss
  where ss_sold_date_sk>=2452617 and ss_sold_date_sk<=2452638
  group by ss_sold_date_sk,
    ss_net_paid_inc_tax,
    ss_net_profit
    distribute by ss_sold_date_sk;

insert overwrite table ss_part partition (ss_sold_date_sk)
select ss_net_paid_inc_tax,
  ss_net_profit,
  ss_sold_date_sk
  from ss
  where ss_sold_date_sk>=2452617 and ss_sold_date_sk<=2452638
  group by ss_sold_date_sk,
    ss_net_paid_inc_tax,
    ss_net_profit
    distribute by ss_sold_date_sk;

desc formatted ss_part partition(ss_sold_date_sk=2452617);
select * from ss_part where ss_sold_date_sk=2452617;

desc formatted ss_part partition(ss_sold_date_sk=2452638);
select * from ss_part where ss_sold_date_sk=2452638;

explain insert overwrite table ss_part partition (ss_sold_date_sk)
select ss_net_paid_inc_tax,
  ss_net_profit,
  ss_sold_date_sk
  from ss
  where ss_sold_date_sk>=2452617 and ss_sold_date_sk<=2452638
    distribute by ss_sold_date_sk;

insert overwrite table ss_part partition (ss_sold_date_sk)
select ss_net_paid_inc_tax,
  ss_net_profit,
  ss_sold_date_sk
  from ss
  where ss_sold_date_sk>=2452617 and ss_sold_date_sk<=2452638
    distribute by ss_sold_date_sk;

desc formatted ss_part partition(ss_sold_date_sk=2452617);
select * from ss_part where ss_sold_date_sk=2452617;

desc formatted ss_part partition(ss_sold_date_sk=2452638);
select * from ss_part where ss_sold_date_sk=2452638;

set hive.optimize.sort.dynamic.partition=false;
-- SORT DYNAMIC PARTITION DISABLED

explain insert overwrite table ss_part partition (ss_sold_date_sk)
select ss_net_paid_inc_tax,
  ss_net_profit,
  ss_sold_date_sk
  from ss
  where ss_sold_date_sk>=2452617 and ss_sold_date_sk<=2452638
  group by ss_sold_date_sk,
    ss_net_paid_inc_tax,
    ss_net_profit
    distribute by ss_sold_date_sk;

insert overwrite table ss_part partition (ss_sold_date_sk)
select ss_net_paid_inc_tax,
  ss_net_profit,
  ss_sold_date_sk
  from ss
  where ss_sold_date_sk>=2452617 and ss_sold_date_sk<=2452638
  group by ss_sold_date_sk,
    ss_net_paid_inc_tax,
    ss_net_profit
    distribute by ss_sold_date_sk;

desc formatted ss_part partition(ss_sold_date_sk=2452617);
select * from ss_part where ss_sold_date_sk=2452617;

desc formatted ss_part partition(ss_sold_date_sk=2452638);
select * from ss_part where ss_sold_date_sk=2452638;

explain insert overwrite table ss_part partition (ss_sold_date_sk)
select ss_net_paid_inc_tax,
  ss_net_profit,
  ss_sold_date_sk
  from ss
  where ss_sold_date_sk>=2452617 and ss_sold_date_sk<=2452638
    distribute by ss_sold_date_sk;

insert overwrite table ss_part partition (ss_sold_date_sk)
select ss_net_paid_inc_tax,
  ss_net_profit,
  ss_sold_date_sk
  from ss
  where ss_sold_date_sk>=2452617 and ss_sold_date_sk<=2452638
    distribute by ss_sold_date_sk;

desc formatted ss_part partition(ss_sold_date_sk=2452617);
select * from ss_part where ss_sold_date_sk=2452617;

desc formatted ss_part partition(ss_sold_date_sk=2452638);
select * from ss_part where ss_sold_date_sk=2452638;

set hive.vectorized.execution.enabled=true;
-- VECTORIZATION IS ENABLED

create table ss_orc (
ss_sold_date_sk int,
ss_net_paid_inc_tax float,
ss_net_profit float) stored as orc;

create table ss_part_orc (
ss_net_paid_inc_tax float,
ss_net_profit float)
partitioned by (ss_sold_date_sk int) stored as orc;

insert overwrite table ss_orc select * from ss;

drop table ss;
drop table ss_part;

explain insert overwrite table ss_part_orc partition (ss_sold_date_sk)
select ss_net_paid_inc_tax,
  ss_net_profit,
  ss_sold_date_sk
  from ss_orc
  where ss_sold_date_sk>=2452617 and ss_sold_date_sk<=2452638
  group by ss_sold_date_sk,
    ss_net_paid_inc_tax,
    ss_net_profit
    distribute by ss_sold_date_sk;

insert overwrite table ss_part_orc partition (ss_sold_date_sk)
select ss_net_paid_inc_tax,
  ss_net_profit,
  ss_sold_date_sk
  from ss_orc
  where ss_sold_date_sk>=2452617 and ss_sold_date_sk<=2452638
  group by ss_sold_date_sk,
    ss_net_paid_inc_tax,
    ss_net_profit
    distribute by ss_sold_date_sk;

desc formatted ss_part_orc partition(ss_sold_date_sk=2452617);
select * from ss_part_orc where ss_sold_date_sk=2452617;

desc formatted ss_part_orc partition(ss_sold_date_sk=2452638);
select * from ss_part_orc where ss_sold_date_sk=2452638;

explain insert overwrite table ss_part_orc partition (ss_sold_date_sk)
select ss_net_paid_inc_tax,
  ss_net_profit,
  ss_sold_date_sk
  from ss_orc
  where ss_sold_date_sk>=2452617 and ss_sold_date_sk<=2452638
    distribute by ss_sold_date_sk;

insert overwrite table ss_part_orc partition (ss_sold_date_sk)
select ss_net_paid_inc_tax,
  ss_net_profit,
  ss_sold_date_sk
  from ss_orc
  where ss_sold_date_sk>=2452617 and ss_sold_date_sk<=2452638
    distribute by ss_sold_date_sk;

desc formatted ss_part_orc partition(ss_sold_date_sk=2452617);
select * from ss_part_orc where ss_sold_date_sk=2452617;

desc formatted ss_part_orc partition(ss_sold_date_sk=2452638);
select * from ss_part_orc where ss_sold_date_sk=2452638;

drop table ss_orc;
drop table ss_part_orc;

drop table if exists hive13_dp1;
create table if not exists hive13_dp1 (
    k1 int,
    k2 int
)
PARTITIONED BY(`day` string)
STORED AS ORC;

set hive.optimize.sort.dynamic.partition=false;
explain insert overwrite table `hive13_dp1` partition(`day`)
select
    key k1,
    count(value) k2,
    "day" `day`
from src
group by "day", key;

insert overwrite table `hive13_dp1` partition(`day`)
select
    key k1,
    count(value) k2,
    "day" `day`
from src
group by "day", key;
select * from hive13_dp1 order by k1, k2 limit 5;

set hive.optimize.sort.dynamic.partition=true;
explain insert overwrite table `hive13_dp1` partition(`day`)
select
    key k1,
    count(value) k2,
    "day" `day`
from src
group by "day", key;

insert overwrite table `hive13_dp1` partition(`day`)
select 
    key k1,
    count(value) k2,
    "day" `day`
from src
group by "day", key;

select * from hive13_dp1 order by k1, k2 limit 5;

drop table hive13_dp1;
