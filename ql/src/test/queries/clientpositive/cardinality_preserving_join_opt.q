create table if not exists customer
(
    c_customer_sk             int,
    c_customer_id             int,
    c_first_name              string,
    c_last_name               string,
    c_birth_country           string,
    c_num                     int
);

create table store_sales
(
    ss_customer_sk            int,
    ss_customer_id            int,
    ss_quantity               int,
    ss_list_price             float
);

alter table customer add constraint pk_c primary key (c_customer_sk, c_customer_id) disable novalidate rely;


insert into customer(c_customer_sk, c_customer_id, c_first_name, c_last_name, c_birth_country, c_num)
values (1, 11, 'John', 'Doe', 'Unknown', 100);

insert into store_sales(ss_customer_sk, ss_customer_id, ss_quantity, ss_list_price)
values (1, 11, 132, 10.5);


-- Test Order of projected columns not match the order defined in the create table statements
explain cbo
select c_num, ss_quantity, c_customer_sk, c_customer_id, ss_customer_sk, ss_customer_id
from store_sales
join customer on ss_customer_sk = c_customer_sk and ss_customer_id = c_customer_id
;

select c_num, ss_quantity, c_customer_sk, c_customer_id, ss_customer_sk, ss_customer_id
from store_sales ss
join customer c on ss.ss_customer_sk = c.c_customer_sk and ss_customer_id = c_customer_id
;


-- Test joining back both tables
alter table store_sales add constraint pk_c primary key (ss_customer_sk, ss_customer_id) disable novalidate rely;

explain cbo
select c_num, ss_quantity, c_customer_sk, c_customer_id, ss_customer_sk, ss_customer_id
from store_sales
join customer on ss_customer_sk = c_customer_sk and ss_customer_id = c_customer_id
;

select c_num, ss_quantity, c_customer_sk, c_customer_id, ss_customer_sk, ss_customer_id
from store_sales
join customer on ss_customer_sk = c_customer_sk and ss_customer_id = c_customer_id
;
