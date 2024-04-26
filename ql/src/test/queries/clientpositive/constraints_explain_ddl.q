set hive.cbo.enable = True;
set hive.vectorized.execution.enabled = True;

CREATE TABLE `customer_removal_n0`(
  `c_custkey` bigint,
  `c_name` string,
  `c_address` string,
  `c_city` string,
  `c_nation` string,
  `c_region` string,
  `c_phone` string,
  `c_mktsegment` string,
primary key (`c_custkey`) disable rely);

CREATE TABLE `dates_removal_n0`(
  `d_datekey` bigint,
  `d_id` bigint,
  `d_date` string,
  `d_dayofweek` string,
  `d_month` string,
  `d_year` int,
  `d_yearmonthnum` int,
  `d_yearmonth` string,
  `d_daynuminweek` int,
  `d_daynuminmonth` int,
  `d_daynuminyear` int,
  `d_monthnuminyear` int,
  `d_weeknuminyear` int,
  `d_sellingseason` string,
  `d_lastdayinweekfl` int,
  `d_lastdayinmonthfl` int,
  `d_holidayfl` int ,
  `d_weekdayfl`int,
primary key (`d_datekey`, `d_id`) disable rely);

-- group by key has single primary key
explain ddl SELECT c_custkey from customer_removal_n0 where c_nation IN ('USA', 'INDIA') group by c_custkey;

-- mix of primary + non-primary keys
explain ddl SELECT c_custkey from customer_removal_n0 where c_nation IN ('USA', 'INDIA') group by c_custkey, c_nation;

-- multiple keys
explain ddl SELECT d_datekey from dates_removal_n0 where d_year IN (1985, 2004) group by d_datekey, d_id;

-- multiple keys + non-keys + different order
explain ddl SELECT d_datekey from dates_removal_n0 where d_year IN (1985, 2004) group by d_id, d_datekey, d_sellingseason
order by d_datekey limit 10;

-- multiple keys in different order and mixed with non-keys
explain ddl SELECT d_datekey from dates_removal_n0 where d_year IN (1985, 2004) group by d_id, d_daynuminmonth, d_datekey,
d_sellingseason order by d_datekey limit 10;

-- same as above but with aggregate
explain ddl SELECT count(d_datekey) from dates_removal_n0 where d_year IN (1985, 2004) group by d_id, d_daynuminmonth, d_datekey,
d_sellingseason order by d_datekey limit 10;

-- join
insert into dates_removal_n0(d_datekey, d_id)  values(3, 0);
insert into dates_removal_n0(d_datekey, d_id)  values(3, 1);
insert into customer_removal_n0 (c_custkey) values(3);

explain ddl SELECT d_datekey from dates_removal_n0 join customer_removal_n0 on d_datekey = c_custkey group by d_datekey, d_id;

-- group by keys are not primary keys
explain ddl SELECT d_datekey from dates_removal_n0 where d_year IN (1985, 2004) group by d_datekey, d_sellingseason
order by d_datekey limit 10;

-- negative
-- with aggregate function
explain ddl SELECT count(c_custkey) from customer_removal_n0 where c_nation IN ('USA', 'INDIA')
group by c_custkey, c_nation;

DROP TABLE customer_removal_n0;
DROP TABLE dates_removal_n0;

-- group by reduction optimization
create table dest_g21 (key1 int, value1 double, primary key(key1) disable rely);
insert into dest_g21 values(1, 2), (2,2), (3, 1), (4,4), (5, null), (6, null);

-- value1 will removed because it is unused, then whole group by will be removed because key1 is unique
explain ddl select key1 from dest_g21 group by key1, value1;
-- same query but with filter
explain ddl select key1 from dest_g21 where value1 > 1 group by key1, value1;

explain ddl select key1 from dest_g21 where key1 > 1 group by key1, value1;

-- only value1 will be removed because there is aggregate call
explain ddl select count(key1) from dest_g21 group by key1, value1;

explain ddl select count(key1) from dest_g21 where value1 > 1 group by key1, value1;

-- t1.key is unique even after join therefore group by = group by (t1.key)
explain ddl select t1.key1 from dest_g21 t1 join dest_g21 t2 on t1.key1 = t2.key1 where t2.value1 > 2 group by t1.key1, t1.value1;

explain ddl select count(t1.key1) from dest_g21 t1 join dest_g21 t2 on t1.key1 = t2.key1 where t2.value1 > 2 group by t1.key1, t1.value1;

-- both aggregate and one of the key1 should be removed
explain ddl select key1 from (select key1, count(key1) from dest_g21 where value1 < 4.5 group by key1, value1) sub;

-- one of the aggregate will be removed and one of the key1 will be removed
explain ddl select key1, sm from (select key1, count(key1), sum(key1) as sm from dest_g21 where value1 < 4.5 group by key1, value1) sub;

DROP table dest_g21;

CREATE TABLE tconst(i int NOT NULL disable rely, j INT NOT NULL disable norely, d_year string);
INSERT INTO tconst values(1, 1, '2001'), (2, null, '2002'), (3, 3, '2010');

-- explicit NOT NULL filter
explain ddl select i, j from tconst where i is not null group by i,j, d_year;

-- filter on i should be removed
explain ddl select i, j from tconst where i IS NOT NULL and j IS NOT NULL group by i,j, d_year;

-- where will be removed since i is not null is always true
explain ddl select i,j from tconst where i is not null OR j IS NOT NULL group by i, j, d_year;

-- should not have implicit filter on join keys
explain ddl select sum(t1.i) from tconst t1 join tconst t2 on t1.i=t2.j group by t1.i, t1.d_year;

-- both join keys have NOT NULL
explain ddl select sum(t1.i) from tconst t1 join tconst t2 on t1.i=t2.i group by t1.i, t1.d_year;

DROP TABLE tconst;


-- UNIQUE + NOT NULL (same as primary key)
create table dest_g21 (key1 int NOT NULL disable rely, value1 double, UNIQUE(key1) disable rely);
explain ddl select key1 from dest_g21 group by key1, value1;

-- UNIQUE with nullabiity
create table dest_g24 (key1 int , value1 double, UNIQUE(key1) disable rely);
explain ddl select key1 from dest_g24 group by key1, value1;

DROP TABLE dest_g21;
DROP TABLE dest_g24;

CREATE TABLE `customer`(
  `c_customer_sk` int,
  `c_customer_id` string,
  `c_current_cdemo_sk` int,
  `c_current_hdemo_sk` int,
  `c_current_addr_sk` int,
  `c_first_shipto_date_sk` int,
  `c_first_sales_date_sk` int,
  `c_salutation` string,
  `c_first_name` string,
  `c_last_name` string,
  `c_preferred_cust_flag` string,
  `c_birth_day` int,
  `c_birth_month` int,
  `c_birth_year` int,
  `c_birth_country` string,
  `c_login` string,
  `c_email_address` string,
  `c_last_review_date` string);

CREATE TABLE `store_sales`(
  `ss_sold_date_sk` int,
  `ss_sold_time_sk` int,
  `ss_item_sk` int,
  `ss_customer_sk` int,
  `ss_cdemo_sk` int,
  `ss_hdemo_sk` int,
  `ss_addr_sk` int,
  `ss_store_sk` int,
  `ss_promo_sk` int,
  `ss_ticket_number` int,
  `ss_quantity` int,
  `ss_wholesale_cost` decimal(7,2),
  `ss_list_price` decimal(7,2),
  `ss_sales_price` decimal(7,2),
  `ss_ext_discount_amt` decimal(7,2),
  `ss_ext_sales_price` decimal(7,2),
  `ss_ext_wholesale_cost` decimal(7,2),
  `ss_ext_list_price` decimal(7,2),
  `ss_ext_tax` decimal(7,2),
  `ss_coupon_amt` decimal(7,2),
  `ss_net_paid` decimal(7,2),
  `ss_net_paid_inc_tax` decimal(7,2),
  `ss_net_profit` decimal(7,2));

alter table customer add constraint pk_c primary key (c_customer_sk) disable novalidate rely;
alter table customer change column c_customer_id c_customer_id string constraint cid_nn not null disable novalidate rely;
alter table customer add constraint uk1 UNIQUE(c_customer_id) disable novalidate rely;

alter table store_sales add constraint pk_ss primary key (ss_item_sk, ss_ticket_number) disable novalidate rely;
alter table store_sales add constraint ss_c foreign key  (ss_customer_sk) references customer (c_customer_sk) disable novalidate rely;

explain ddl
select c_customer_id
from customer
,store_sales
where c_customer_sk = ss_customer_sk
group by c_customer_id
,c_first_name
,c_last_name
,c_preferred_cust_flag
,c_birth_country
,c_login
,c_email_address;

explain ddl
select c_customer_id
from store_sales
,customer
where c_customer_sk = ss_customer_sk
group by c_customer_id
,c_first_name
,c_last_name
,c_preferred_cust_flag
,c_birth_country
,c_login
,c_email_address;

explain ddl with year_total as (
  select c_customer_id customer_id
  ,c_first_name customer_first_name
  ,c_last_name customer_last_name
  ,c_preferred_cust_flag customer_preferred_cust_flag
  ,c_birth_country customer_birth_country
  ,c_login customer_login
  ,c_email_address customer_email_address
  ,sum(((ss_ext_list_price-ss_ext_wholesale_cost-ss_ext_discount_amt)+ss_ext_sales_price)/2) year_total
  ,'s' sale_type
  from customer
  ,store_sales
  where c_customer_sk = ss_customer_sk
  group by c_customer_id
  ,c_first_name
  ,c_last_name
  ,c_preferred_cust_flag
  ,c_birth_country
  ,c_login
  ,c_email_address
)
select  t_s_secyear.customer_preferred_cust_flag
from
year_total t_s_secyear
where t_s_secyear.sale_type = 's'
order by t_s_secyear.customer_preferred_cust_flag
limit 100;

explain ddl
with year_total as (
  select c_customer_id customer_id
  ,c_first_name customer_first_name
  ,c_last_name customer_last_name
  ,c_preferred_cust_flag customer_preferred_cust_flag
  ,c_birth_country customer_birth_country
  ,c_login customer_login
  ,c_email_address customer_email_address
  ,sum(((ss_ext_list_price-ss_ext_wholesale_cost-ss_ext_discount_amt)+ss_ext_sales_price)/2) year_total
  ,'s' sale_type
  from store_sales
  ,customer
  where c_customer_sk = ss_customer_sk
  group by c_customer_id
  ,c_first_name
  ,c_last_name
  ,c_preferred_cust_flag
  ,c_birth_country
  ,c_login
  ,c_email_address
)
select  t_s_secyear.customer_preferred_cust_flag
from
year_total t_s_secyear
where t_s_secyear.sale_type = 's'
order by t_s_secyear.customer_preferred_cust_flag
limit 100;

CREATE TABLE `date_dim`(
  `d_date_sk` int,
  `d_date_id` string,
  `d_date` string,
  `d_month_seq` int,
  `d_week_seq` int,
  `d_quarter_seq` int,
  `d_year` int,
  `d_dow` int,
  `d_moy` int,
  `d_dom` int,
  `d_qoy` int,
  `d_fy_year` int,
  `d_fy_quarter_seq` int,
  `d_fy_week_seq` int,
  `d_day_name` string,
  `d_quarter_name` string,
  `d_holiday` string,
  `d_weekend` string,
  `d_following_holiday` string,
  `d_first_dom` int,
  `d_last_dom` int,
  `d_same_day_ly` int,
  `d_same_day_lq` int,
  `d_current_day` string,
  `d_current_week` string,
  `d_current_month` string,
  `d_current_quarter` string,
  `d_current_year` string);

explain ddl
with year_total as (
  select c_customer_id customer_id
  ,c_first_name customer_first_name
  ,c_last_name customer_last_name
  ,c_preferred_cust_flag customer_preferred_cust_flag
  ,c_birth_country customer_birth_country
  ,c_login customer_login
  ,c_email_address customer_email_address
  ,d_year dyear
  ,sum(((ss_ext_list_price-ss_ext_wholesale_cost-ss_ext_discount_amt)+ss_ext_sales_price)/2) year_total
  ,'s' sale_type
  from customer
  ,store_sales
  ,date_dim
  where c_customer_sk = ss_customer_sk
  and ss_sold_date_sk = d_date_sk
  group by c_customer_id
  ,c_first_name
  ,c_last_name
  ,c_preferred_cust_flag
  ,c_birth_country
  ,c_login
  ,c_email_address
  ,d_year
)
select  t_s_secyear.customer_preferred_cust_flag
from year_total t_s_firstyear
,year_total t_s_secyear
where t_s_secyear.customer_id = t_s_firstyear.customer_id
and t_s_firstyear.sale_type = 's'
and t_s_secyear.sale_type = 's'
and t_s_firstyear.dyear =  2001
and t_s_secyear.dyear = 2001+1
and t_s_firstyear.year_total > 0
order by t_s_secyear.customer_preferred_cust_flag
limit 100;

-- group by is both on unique as well pk but pk is used
explain ddl
SELECT
C_CUSTOMER_SK
FROM
CUSTOMER
,	STORE_SALES
WHERE
C_CUSTOMER_SK	=	SS_CUSTOMER_SK
GROUP BY
C_CUSTOMER_SK
,	C_CUSTOMER_ID
,	C_FIRST_NAME
,	C_LAST_NAME
,	C_PREFERRED_CUST_FLAG
,	C_BIRTH_COUNTRY
,	C_LOGIN
,	C_EMAIL_ADDRESS
;
-- group by is both on unique as well pk but unique is used
explain ddl
SELECT
C_CUSTOMER_ID
FROM
CUSTOMER
,	STORE_SALES
WHERE
C_CUSTOMER_SK	=	SS_CUSTOMER_SK
GROUP BY
C_CUSTOMER_SK
,	C_CUSTOMER_ID
,	C_FIRST_NAME
,	C_LAST_NAME
,	C_PREFERRED_CUST_FLAG
,	C_BIRTH_COUNTRY
,	C_LOGIN
,	C_EMAIL_ADDRESS
;
-- should keep the unique + one column c_first_name in gby
explain ddl
SELECT
C_FIRST_NAME
FROM
CUSTOMER
,	STORE_SALES
WHERE
C_CUSTOMER_SK	=	SS_CUSTOMER_SK
GROUP BY
C_CUSTOMER_SK
,	C_FIRST_NAME
,	C_LAST_NAME
,	C_PREFERRED_CUST_FLAG
,	C_BIRTH_COUNTRY
,	C_LOGIN
,	C_EMAIL_ADDRESS
;

-- group by keys order is different than than the source
explain ddl
SELECT
C_CUSTOMER_ID
FROM
CUSTOMER
,	STORE_SALES
WHERE
C_CUSTOMER_SK	=	SS_CUSTOMER_SK
GROUP BY
C_EMAIL_ADDRESS
,	C_LAST_NAME
,	C_FIRST_NAME
,	C_CUSTOMER_ID
,	C_PREFERRED_CUST_FLAG
,	C_BIRTH_COUNTRY
,	C_LOGIN
;

create table web_sales(ws_order_number int, ws_item_sk int, ws_price float,
constraint pk1 primary key(ws_order_number, ws_item_sk) disable rely);
insert into web_sales values(1, 1, 1.2);
insert into web_sales values(1, 1, 1.2);
explain ddl select count(distinct ws_order_number) from web_sales;
drop table web_sales;

CREATE TABLE table1_n13 (a STRING, b STRING, PRIMARY KEY (a) DISABLE);
CREATE TABLE table2_n8 (a STRING, b STRING, CONSTRAINT pk1 PRIMARY KEY (a) DISABLE);
CREATE TABLE table3_n1 (x string NOT NULL DISABLE, PRIMARY KEY (x) DISABLE, CONSTRAINT fk1 FOREIGN KEY (x) REFERENCES table2_n8(a) DISABLE);
CREATE TABLE table4_n0 (x string CONSTRAINT nn4_1 NOT NULL DISABLE, y string CONSTRAINT nn4_2 NOT NULL DISABLE, UNIQUE (x) DISABLE, CONSTRAINT fk2 FOREIGN KEY (x) REFERENCES table2_n8(a) DISABLE,
CONSTRAINT fk3 FOREIGN KEY (y) REFERENCES table2_n8(a) DISABLE);
CREATE TABLE table5_n4 (x string, PRIMARY KEY (x) DISABLE, FOREIGN KEY (x) REFERENCES table2_n8(a) DISABLE);
CREATE TABLE table6_n3 (x string, y string, PRIMARY KEY (x) DISABLE, FOREIGN KEY (x) REFERENCES table2_n8(a) DISABLE,
CONSTRAINT fk4 FOREIGN KEY (y) REFERENCES table1_n13(a) DISABLE);
CREATE TABLE table7_n3 (a STRING, b STRING, PRIMARY KEY (a) DISABLE RELY);
CREATE TABLE table8 (a STRING, b STRING, CONSTRAINT pk8 PRIMARY KEY (a) DISABLE NORELY);
CREATE TABLE table9 (a STRING, b STRING, PRIMARY KEY (a, b) DISABLE RELY);
CREATE TABLE table10 (a STRING, b STRING, CONSTRAINT pk10 PRIMARY KEY (a) DISABLE NORELY, FOREIGN KEY (a, b) REFERENCES table9(a, b) DISABLE);
CREATE TABLE table11 (a STRING, b STRING, c STRING, CONSTRAINT pk11 PRIMARY KEY (a) DISABLE RELY, CONSTRAINT fk11_1 FOREIGN KEY (a, b) REFERENCES table9(a, b) DISABLE,
CONSTRAINT fk11_2 FOREIGN KEY (c) REFERENCES table4_n0(x) DISABLE);
CREATE TABLE table12 (a STRING CONSTRAINT nn12_1 NOT NULL DISABLE NORELY, b STRING);
CREATE TABLE table13 (b STRING) PARTITIONED BY (a STRING NOT NULL DISABLE RELY);
CREATE TABLE table14 (a STRING CONSTRAINT nn14_1 NOT NULL DISABLE RELY, b STRING);
CREATE TABLE table15 (a STRING REFERENCES table4_n0(x) DISABLE, b STRING);
CREATE TABLE table16 (a STRING CONSTRAINT nn16_1 REFERENCES table4_n0(x) DISABLE RELY, b STRING);
CREATE TABLE table17 (a STRING CONSTRAINT uk17_1 UNIQUE DISABLE RELY, b STRING);
CREATE TABLE table18 (a STRING, CONSTRAINT uk18_1 UNIQUE (b) DISABLE RELY) PARTITIONED BY (b STRING);
CREATE TABLE table19 (a STRING, b STRING, CONSTRAINT pk19_1 PRIMARY KEY (b) DISABLE RELY, CONSTRAINT fk19_2 FOREIGN KEY (a) REFERENCES table19(b) DISABLE RELY);
CREATE TABLE table20 (a STRING, b STRING, CONSTRAINT uk20_1 UNIQUE (b) DISABLE RELY, CONSTRAINT fk20_2 FOREIGN KEY (a) REFERENCES table20(b) DISABLE RELY);
CREATE TABLE table21 (a STRING, CONSTRAINT uk21_1 UNIQUE (a,b) DISABLE) PARTITIONED BY (b STRING);
CREATE TABLE table22 (a STRING, b STRING, CONSTRAINT fk22_1 FOREIGN KEY (a,b) REFERENCES table21(a,b) DISABLE);


explain ddl select * from table1_n13;
explain ddl select * from table2_n8;
explain ddl select * from table3_n1;
explain ddl select * from table4_n0;
explain ddl select * from table5_n4;
explain ddl select * from table6_n3;
explain ddl select * from table7_n3;
explain ddl select * from table8;
explain ddl select * from table9;
explain ddl select * from table10;
explain ddl select * from table11;
explain ddl select * from table12;
explain ddl select * from table13;
explain ddl select * from table14;
explain ddl select * from table15;
explain ddl select * from table16;
explain ddl select * from table17;
explain ddl select * from table18;
explain ddl select * from table19;
explain ddl select * from table20;
explain ddl select * from table21;
explain ddl select * from table22;

explain ddl select * from table1_n13;
explain ddl select * from table2_n8;
explain ddl select * from table3_n1;
explain ddl select * from table4_n0;
explain ddl select * from table5_n4;
explain ddl select * from table6_n3;
explain ddl select * from table7_n3;
explain ddl select * from table8;
explain ddl select * from table9;
explain ddl select * from table10;
explain ddl select * from table11;
explain ddl select * from table12;
explain ddl select * from table13;
explain ddl select * from table14;
explain ddl select * from table15;
explain ddl select * from table16;
explain ddl select * from table17;
explain ddl select * from table18;
explain ddl select * from table19;
explain ddl select * from table20;
explain ddl select * from table21;
explain ddl select * from table22;

ALTER TABLE table2_n8 DROP CONSTRAINT pk1;
ALTER TABLE table3_n1 DROP CONSTRAINT fk1;
ALTER TABLE table4_n0 DROP CONSTRAINT nn4_1;
ALTER TABLE table6_n3 DROP CONSTRAINT fk4;
ALTER TABLE table8 DROP CONSTRAINT pk8;
ALTER TABLE table16 DROP CONSTRAINT nn16_1;
ALTER TABLE table18 DROP CONSTRAINT uk18_1;

explain ddl select * from table2_n8;
explain ddl select * from table3_n1;
explain ddl select * from table4_n0;
explain ddl select * from table6_n3;
explain ddl select * from table8;
explain ddl select * from table16;
explain ddl select * from table18;

explain ddl select * from table2_n8;
explain ddl select * from table3_n1;
explain ddl select * from table4_n0;
explain ddl select * from table6_n3;
explain ddl select * from table8;
explain ddl select * from table16;
explain ddl select * from table18;

ALTER TABLE table2_n8 ADD CONSTRAINT pkt2 PRIMARY KEY (a) DISABLE NOVALIDATE;
ALTER TABLE table3_n1 ADD CONSTRAINT fk1 FOREIGN KEY (x) REFERENCES table2_n8(a) DISABLE NOVALIDATE RELY;
ALTER TABLE table6_n3 ADD CONSTRAINT fk4 FOREIGN KEY (y) REFERENCES table1_n13(a) DISABLE NOVALIDATE;
ALTER TABLE table8 ADD CONSTRAINT pk8_2 PRIMARY KEY (a, b) DISABLE NOVALIDATE RELY;
ALTER TABLE table16 CHANGE a a STRING REFERENCES table4_n0(x) DISABLE NOVALIDATE;
ALTER TABLE table18 ADD CONSTRAINT uk18_2 UNIQUE (a, b) DISABLE NOVALIDATE;

explain ddl select * from table2_n8;
explain ddl select * from table3_n1;
explain ddl select * from table6_n3;
explain ddl select * from table8;
explain ddl select * from table16;
explain ddl select * from table18;

ALTER TABLE table12 CHANGE COLUMN b b STRING CONSTRAINT nn12_2 NOT NULL DISABLE NOVALIDATE;
ALTER TABLE table13 CHANGE b b STRING NOT NULL DISABLE NOVALIDATE;

explain ddl select * from table12;
explain ddl select * from table13;

ALTER TABLE table12 DROP CONSTRAINT nn12_2;

explain ddl select * from table12;

CREATE DATABASE DbConstraint;
USE DbConstraint;
CREATE TABLE Table2 (a STRING, b STRING NOT NULL DISABLE, CONSTRAINT Pk1 PRIMARY KEY (a) DISABLE);
USE default;

explain ddl select * from DbConstraint.Table2;
explain ddl select * from DbConstraint.Table2;

ALTER TABLE DbConstraint.Table2 DROP CONSTRAINT Pk1;

explain ddl select * from DbConstraint.Table2;
explain ddl select * from DbConstraint.Table2;

ALTER TABLE DbConstraint.Table2 ADD CONSTRAINT Pk1 PRIMARY KEY (a) DISABLE NOVALIDATE;
explain ddl select * from DbConstraint.Table2;
ALTER TABLE DbConstraint.Table2 ADD CONSTRAINT fkx FOREIGN KEY (b) REFERENCES table1_n13(a) DISABLE NOVALIDATE;
explain ddl select * from DbConstraint.Table2;

CREATE TABLE table23 (a STRING) PARTITIONED BY (b STRING);

ALTER TABLE table23 ADD CONSTRAINT fk23_1 FOREIGN KEY (a,b) REFERENCES table21(a,b) DISABLE NOVALIDATE RELY;
ALTER TABLE table23 ADD CONSTRAINT pk23_1 PRIMARY KEY (b) DISABLE RELY;

explain ddl select * from table23;