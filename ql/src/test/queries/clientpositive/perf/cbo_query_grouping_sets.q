EXPLAIN CBO
select  
        ca_country, ca_state, i_item_id,
         avg( cast(cs_quantity as numeric(12,2))) agg1,
         avg( cast(c_birth_year as numeric(12,2))) agg6,
         avg( cast(cd1.cd_dep_count as numeric(12,2))) agg7
from catalog_sales, customer_demographics cd1,
        customer, customer_address,
        date_dim,
        item
where cs_sold_date_sk = d_date_sk and
        cs_item_sk = i_item_sk and
        cs_bill_cdemo_sk = cd1.cd_demo_sk and
        cs_bill_customer_sk = c_customer_sk and
        cd1.cd_gender = 'M' and
        cd1.cd_education_status = 'College' and
        c_current_addr_sk = ca_address_sk and
        c_birth_month in (9,5) and
        d_year = 2001 and
        ca_state in ('AL','MS','TN')
group by rollup(i_item_id, ca_country, ca_state)
order by ca_country, ca_state, i_item_id NULLS FIRST
limit 100;

set hive.explain.user=false;

EXPLAIN
select  
        ca_country, ca_state, i_item_id,
         avg( cast(cs_quantity as numeric(12,2))) agg1,
         avg( cast(c_birth_year as numeric(12,2))) agg6,
         avg( cast(cd1.cd_dep_count as numeric(12,2))) agg7
from catalog_sales, customer_demographics cd1,
        customer, customer_address,
        date_dim,
        item
where cs_sold_date_sk = d_date_sk and
        cs_item_sk = i_item_sk and
        cs_bill_cdemo_sk = cd1.cd_demo_sk and
        cs_bill_customer_sk = c_customer_sk and
        cd1.cd_gender = 'M' and
        cd1.cd_education_status = 'College' and
        c_current_addr_sk = ca_address_sk and
        c_birth_month in (9,5) and
        d_year = 2001 and
        ca_state in ('AL','MS','TN')
group by rollup(i_item_id, ca_country, ca_state)
order by ca_country, ca_state, i_item_id NULLS FIRST
limit 100;
