create table if not exists customer
(
    c_first_name            string,
    c_last_name             string,
    c_customer_sk           bigint,
    c_discount              float
);

create table store_sales
(
    ss_quantity            float,
    ss_customer_sk         int,
    ss_list_price          float
);

insert into customer(c_customer_sk, c_first_name, c_last_name, c_discount) values (1, 'John', 'Doe', 0.15);
insert into store_sales(ss_customer_sk, ss_quantity, ss_list_price) values (1, 10.0, 2.5);

-- Turn off optimization to have a reference
set hive.cardinality.preserving.join.optimization.factor=0.0;

explain cbo
select
    c_first_name,
    c_first_name || ' ' || c_last_name || ' ' || c_last_name,
    ((ss_quantity + ss_quantity) * ss_list_price) * (1.0 - c_discount),
    c_customer_sk,
    ss_customer_sk
from store_sales ss
join customer c on ss_customer_sk = c_customer_sk;

select
    c_first_name,
    c_first_name || ' ' || c_last_name || ' ' || c_last_name,
    ((ss_quantity + ss_quantity) * ss_list_price) * (1.0 - c_discount),
    c_customer_sk,
    ss_customer_sk
from store_sales ss
join customer c on ss_customer_sk = c_customer_sk;

-- Force optimization
set hive.cardinality.preserving.join.optimization.factor=10.0;

-- Only store_sales table should be joined back
alter table store_sales add constraint pk_c primary key (ss_customer_sk) disable novalidate rely;

explain cbo
select
    c_first_name,
    c_first_name || ' ' || c_last_name || ' ' || c_last_name,
    ((ss_quantity + ss_quantity) * ss_list_price) * (1.0 - c_discount),
    c_customer_sk,
    ss_customer_sk
from store_sales ss
join customer c on ss_customer_sk = c_customer_sk;

select
    c_first_name,
    c_first_name || ' ' || c_last_name || ' ' || c_last_name,
    ((ss_quantity + ss_quantity) * ss_list_price) * (1.0 - c_discount),
    c_customer_sk,
    ss_customer_sk
from store_sales ss
join customer c on ss_customer_sk = c_customer_sk;


-- Only customer table should be joined back
alter table store_sales drop constraint pk_c;
alter table customer add constraint pk_c primary key (c_customer_sk) disable novalidate rely;

explain cbo
select
    c_first_name,
    c_first_name || ' ' || c_last_name || ' ' || c_last_name,
    ((ss_quantity + ss_quantity) * ss_list_price) * (1.0 - c_discount),
    c_customer_sk,
    ss_customer_sk
from store_sales ss
join customer c on ss_customer_sk = c_customer_sk;

select
    c_first_name,
    c_first_name || ' ' || c_last_name || ' ' || c_last_name,
    ((ss_quantity + ss_quantity) * ss_list_price) * (1.0 - c_discount),
    c_customer_sk,
    ss_customer_sk
from store_sales ss
join customer c on ss_customer_sk = c_customer_sk;


-- Both tables should be joined back
alter table store_sales add constraint pk_c primary key (ss_customer_sk) disable novalidate rely;

explain cbo
select
    c_first_name,
    c_first_name || ' ' || c_last_name || ' ' || c_last_name,
    ((ss_quantity + ss_quantity) * ss_list_price) * (1.0 - c_discount),
    c_customer_sk,
    ss_customer_sk
from store_sales ss
join customer c on ss_customer_sk = c_customer_sk;

select
    c_first_name,
    c_first_name || ' ' || c_last_name || ' ' || c_last_name,
    ((ss_quantity + ss_quantity) * ss_list_price) * (1.0 - c_discount),
    c_customer_sk,
    ss_customer_sk
from store_sales ss
join customer c on ss_customer_sk = c_customer_sk;

-- Table store_sales is not joined back: only the key is projected
explain cbo
select
    c_first_name,
    ss_customer_sk,
    c_customer_sk
from store_sales ss
join customer on c_customer_sk = ss_customer_sk;

select
    c_first_name,
    ss_customer_sk,
    c_customer_sk
from store_sales ss
join customer on c_customer_sk = ss_customer_sk;
