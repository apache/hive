
CREATE TABLE package_order (
    order_num string,
    vin_id string,
    package_start_dt string);

CREATE TABLE package_order_gsp (order_num string,
                    cust_acct_sk decimal(38,0),
                     to_vin string,
                      cancellation_dt string,
                       confirmation_num string);

insert into package_order values ('1', 'DEADBEAF', '2022-01-22');
insert into package_order values ('1', 'DEADBEAF', '2022-01-22');
insert into package_order values ('1', 'DEADBEAF1', '2022-01-23');
insert into package_order values ('1', 'DEADBEAF1', '2022-01-23');
insert into package_order_gsp values ('1', 1.1, '1', null, '1');

set hive.cbo.enable = false;
set hive.explain.user=false;

explain select *
from (
select t1.vin_id, row_number()over(partition by t1.vin_id order by package_start_dt desc) rn
from package_order_gsp su
inner join package_order t1
on su.confirmation_num=t1.order_num
where su.cancellation_dt is null
) tt
where tt.vin_id='DEADBEAF';

select *
from (
select t1.vin_id, row_number()over(partition by t1.vin_id order by package_start_dt desc) rn
from package_order_gsp su
inner join package_order t1
on su.confirmation_num=t1.order_num
where su.cancellation_dt is null
) tt
where tt.vin_id='DEADBEAF';

set hive.cbo.enable = true;

explain select *
from (
select t1.vin_id, row_number()over(partition by t1.vin_id order by package_start_dt desc) rn
from package_order_gsp su
inner join package_order t1
on su.confirmation_num=t1.order_num
where su.cancellation_dt is null
) tt
where tt.vin_id='DEADBEAF';

select *
from (
select t1.vin_id, row_number()over(partition by t1.vin_id order by package_start_dt desc) rn
from package_order_gsp su
inner join package_order t1
on su.confirmation_num=t1.order_num
where su.cancellation_dt is null
) tt
where tt.vin_id='DEADBEAF';

select *
from (
select t1.vin_id, row_number()over(partition by t1.vin_id order by package_start_dt desc) rn
from package_order_gsp su
inner join package_order t1
on su.confirmation_num=t1.order_num
where su.cancellation_dt is null
) tt
where tt.vin_id != 'DEADBEAF';

