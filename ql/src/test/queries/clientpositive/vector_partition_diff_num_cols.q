SET hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=minimal;

create table inventory_txt
(
    inv_date_sk                int,
    inv_item_sk                int,
    inv_warehouse_sk           int,
    inv_quantity_on_hand       int
)
row format delimited fields terminated by '|' 
stored as textfile;

LOAD DATA LOCAL INPATH '../../data/files/inventory' OVERWRITE INTO TABLE inventory_txt;

-- No column change case

create table inventory_part_0(
    inv_date_sk             int,
    inv_item_sk             int,
    inv_warehouse_sk        int,
    inv_quantity_on_hand    int)
partitioned by (par string) stored as orc;

insert into table inventory_part_0 partition(par='1') select * from inventory_txt;
insert into table inventory_part_0 partition(par='2') select * from inventory_txt;

explain
select sum(inv_quantity_on_hand) from inventory_part_0;

select sum(inv_quantity_on_hand) from inventory_part_0;

-- Additional column for 2nd partition...

create table inventory_part_1(
    inv_date_sk             int,
    inv_item_sk             int,
    inv_warehouse_sk        int,
    inv_quantity_on_hand    int)
partitioned by (par string) stored as orc;

insert into table inventory_part_1 partition(par='4cols') select * from inventory_txt;

alter table inventory_part_1 add columns (fifthcol string);

insert into table inventory_part_1 partition(par='5cols') select *, '5th' as fifthcol from inventory_txt;

explain
select sum(inv_quantity_on_hand) from inventory_part_1;

select sum(inv_quantity_on_hand) from inventory_part_1;

-- Verify we do not vectorize when a partition column name is different.
-- Currently, we do not attempt the actual select because non-vectorized ORC table reader gets a cast exception.

create table inventory_part_2a(
    inv_date_sk             int,
    inv_item_sk             int,
    inv_warehouse_sk        int,
    inv_quantity_on_hand    int)
partitioned by (par string) stored as orc;

insert into table inventory_part_2a partition(par='1') select * from inventory_txt;
insert into table inventory_part_2a partition(par='2') select * from inventory_txt;
alter table inventory_part_2a partition (par='2') change inv_item_sk other_name int;

explain
select sum(inv_quantity_on_hand) from inventory_part_2a;

create table inventory_part_2b(
    inv_date_sk             int,
    inv_item_sk             int,
    inv_warehouse_sk        int,
    inv_quantity_on_hand    int)
partitioned by (par1 string, par2 int) stored as orc;

insert into table inventory_part_2b partition(par1='1',par2=4) select * from inventory_txt;
insert into table inventory_part_2b partition(par1='2',par2=3) select * from inventory_txt;
alter table inventory_part_2b partition (par1='2',par2=3) change inv_quantity_on_hand other_name int;

explain
select sum(inv_quantity_on_hand) from inventory_part_2b;

-- Verify we do not vectorize when a partition column type is different.
-- Currently, we do not attempt the actual select because non-vectorized ORC table reader gets a cast exception.

create table inventory_part_3(
    inv_date_sk             int,
    inv_item_sk             int,
    inv_warehouse_sk        int,
    inv_quantity_on_hand    int)
partitioned by (par string) stored as orc;

insert into table inventory_part_3 partition(par='1') select * from inventory_txt;
insert into table inventory_part_3 partition(par='2') select * from inventory_txt;
alter table inventory_part_3 partition (par='2') change inv_warehouse_sk inv_warehouse_sk bigint;

explain
select sum(inv_quantity_on_hand) from inventory_part_3;