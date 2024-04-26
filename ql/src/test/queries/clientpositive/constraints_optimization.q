set hive.strict.checks.cartesian.product=false;

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
  EXPLAIN SELECT c_custkey from customer_removal_n0 where c_nation IN ('USA', 'INDIA') group by c_custkey;

  -- mix of primary + non-primary keys
  EXPLAIN SELECT c_custkey from customer_removal_n0 where c_nation IN ('USA', 'INDIA') group by c_custkey, c_nation;

  -- multiple keys
  EXPLAIN SELECT d_datekey from dates_removal_n0 where d_year IN (1985, 2004) group by d_datekey, d_id;

  -- multiple keys + non-keys + different order
  EXPLAIN SELECT d_datekey from dates_removal_n0 where d_year IN (1985, 2004) group by d_id, d_datekey, d_sellingseason
    order by d_datekey limit 10;

 -- multiple keys in different order and mixed with non-keys
  EXPLAIN SELECT d_datekey from dates_removal_n0 where d_year IN (1985, 2004) group by d_id, d_daynuminmonth, d_datekey,
  d_sellingseason order by d_datekey limit 10;

  -- same as above but with aggregate
  EXPLAIN SELECT count(d_datekey) from dates_removal_n0 where d_year IN (1985, 2004) group by d_id, d_daynuminmonth, d_datekey,
  d_sellingseason order by d_datekey limit 10;

  -- join
  insert into dates_removal_n0(d_datekey, d_id)  values(3, 0);
  insert into dates_removal_n0(d_datekey, d_id)  values(3, 1);
  insert into customer_removal_n0 (c_custkey) values(3);

  EXPLAIN SELECT d_datekey from dates_removal_n0 join customer_removal_n0 on d_datekey = c_custkey group by d_datekey, d_id;
  SELECT d_datekey from dates_removal_n0 join customer_removal_n0 on d_datekey = c_custkey group by d_datekey, d_id;

  -- group by keys are not primary keys
  EXPLAIN SELECT d_datekey from dates_removal_n0 where d_year IN (1985, 2004) group by d_datekey, d_sellingseason
    order by d_datekey limit 10;

  -- negative
  -- with aggregate function
  EXPLAIN SELECT count(c_custkey) from customer_removal_n0 where c_nation IN ('USA', 'INDIA')
    group by c_custkey, c_nation;

  DROP TABLE customer_removal_n0;
  DROP TABLE dates_removal_n0;

  -- group by reduction optimization
  create table dest_g21 (key1 int, value1 double, primary key(key1) disable rely);
  insert into dest_g21 values(1, 2), (2,2), (3, 1), (4,4), (5, null), (6, null);

  -- value1 will removed because it is unused, then whole group by will be removed because key1 is unique
  explain select key1 from dest_g21 group by key1, value1;
  select key1 from dest_g21 group by key1, value1;
  -- same query but with filter
  explain select key1 from dest_g21 where value1 > 1 group by key1, value1;
  select key1 from dest_g21 where value1 > 1 group by key1, value1;

  explain select key1 from dest_g21 where key1 > 1 group by key1, value1;
  select key1 from dest_g21 where key1 > 1 group by key1, value1;

  -- only value1 will be removed because there is aggregate call
  explain select count(key1) from dest_g21 group by key1, value1;
  select count(key1) from dest_g21 group by key1, value1;

  explain select count(key1) from dest_g21 where value1 > 1 group by key1, value1;
  select count(key1) from dest_g21 where value1 > 1 group by key1, value1;

  -- t1.key is unique even after join therefore group by = group by (t1.key)
  explain select t1.key1 from dest_g21 t1 join dest_g21 t2 on t1.key1 = t2.key1 where t2.value1 > 2 group by t1.key1, t1.value1;
  select t1.key1 from dest_g21 t1 join dest_g21 t2 on t1.key1 = t2.key1 where t2.value1 > 2 group by t1.key1, t1.value1;

  explain select count(t1.key1) from dest_g21 t1 join dest_g21 t2 on t1.key1 = t2.key1 where t2.value1 > 2 group by t1.key1, t1.value1;
  select count(t1.key1) from dest_g21 t1 join dest_g21 t2 on t1.key1 = t2.key1 where t2.value1 > 2 group by t1.key1, t1.value1;

  -- both aggregate and one of the key1 should be removed
  explain select key1 from (select key1, count(key1) from dest_g21 where value1 < 4.5 group by key1, value1) sub;
  select key1 from (select key1, count(key1) from dest_g21 where value1 < 4.5 group by key1, value1) sub;

  -- one of the aggregate will be removed and one of the key1 will be removed
  explain select key1, sm from (select key1, count(key1), sum(key1) as sm from dest_g21 where value1 < 4.5 group by key1, value1) sub;
  select key1, sm from (select key1, count(key1), sum(key1) as sm from dest_g21 where value1 < 4.5 group by key1, value1) sub;

  DROP table dest_g21;

CREATE TABLE tconst(i int NOT NULL disable rely, j INT NOT NULL disable norely, d_year string);
INSERT INTO tconst values(1, 1, '2001'), (2, null, '2002'), (3, 3, '2010');

-- explicit NOT NULL filter
explain select i, j from tconst where i is not null group by i,j, d_year;
select i, j from tconst where i is not null group by i,j, d_year;

-- filter on i should be removed
explain select i, j from tconst where i IS NOT NULL and j IS NOT NULL group by i,j, d_year;
select i, j from tconst where i IS NOT NULL and j IS NOT NULL group by i,j, d_year;

-- where will be removed since i is not null is always true
explain select i,j from tconst where i is not null OR j IS NOT NULL group by i, j, d_year;
select i,j from tconst where i is not null OR j IS NOT NULL group by i, j, d_year;

-- should not have implicit filter on join keys
explain select sum(t1.i) from tconst t1 join tconst t2 on t1.i=t2.j group by t1.i, t1.d_year;
select sum(t1.i) from tconst t1 join tconst t2 on t1.i=t2.j group by t1.i, t1.d_year;

-- both join keys have NOT NULL
explain select sum(t1.i) from tconst t1 join tconst t2 on t1.i=t2.i group by t1.i, t1.d_year;
select sum(t1.i) from tconst t1 join tconst t2 on t1.i=t2.i group by t1.i, t1.d_year;

DROP TABLE tconst;


-- UNIQUE + NOT NULL (same as primary key)
create table dest_g21 (key1 int NOT NULL disable rely, value1 double, UNIQUE(key1) disable rely);
explain select key1 from dest_g21 group by key1, value1;

-- UNIQUE with nullabiity
create table dest_g24 (key1 int , value1 double, UNIQUE(key1) disable rely);
explain select key1 from dest_g24 group by key1, value1;

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

    explain cbo
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

     explain cbo
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

    explain cbo
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

    explain cbo
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

    explain cbo
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
EXPLAIN	CBO
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
EXPLAIN	CBO
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
EXPLAIN	CBO
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
EXPLAIN	CBO
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

-- group by keys with columns from multiple table
explain cbo select c_customer_sk from
 (select c_first_name, c_customer_sk ,d_date solddate,count(*) cnt
  from store_sales
      ,date_dim
      ,customer
  where ss_sold_date_sk = d_date_sk
    and ss_item_sk = c_customer_sk
  group by c_first_name,c_customer_sk,d_date
  having count(*) >4) subq;

-- group by keys from multiple table with expression
explain cbo select c_customer_sk from
 (select substr(c_first_name, 1,30), c_customer_sk ,d_date solddate,count(*) cnt
  from store_sales
      ,date_dim
      ,customer
  where ss_sold_date_sk = d_date_sk
    and ss_item_sk = c_customer_sk
  group by substr(c_first_name, 1, 30),c_customer_sk,d_date
  having count(*) >4) subq;

-- group by keys from same table with expression
 explain cbo select c_customer_sk from
 (select substr(c_first_name, 1,30), c_customer_sk ,count(*) cnt
  from store_sales
      ,date_dim
      ,customer
  where ss_sold_date_sk = d_date_sk
    and ss_item_sk = c_customer_sk
  group by substr(c_first_name, 1, 30),c_customer_sk
  having count(*) >4) subq;

 -- group by keys from multiple table with non-deterministic expression
explain cbo select c_customer_sk from
 (select rand(), c_customer_sk ,d_date solddate,count(*) cnt
  from store_sales
      ,date_dim
      ,customer
  where ss_sold_date_sk = d_date_sk
    and ss_item_sk = c_customer_sk
  group by rand(),c_customer_sk,d_date
  having count(*) >4) subq;

-- group by keys from multiple table with expression on pk itself, group by shouldn't be reduced
explain cbo select * from
 (select substr(c_first_name, 1,30), log2(c_customer_sk),d_date solddate,count(*) cnt
  from store_sales
      ,date_dim
      ,customer
  where ss_sold_date_sk = d_date_sk
    and ss_item_sk = c_customer_sk
  group by substr(c_first_name, 1, 30),log2(c_customer_sk),d_date
  having count(*) >4) subq;

 -- group by with keys consisting of pk from multiple tables with extra columns from both side
 explain cbo select c_customer_sk from
 (select substr(c_first_name, 1,30), c_customer_sk ,d_date solddate, d_date_sk, count(*) cnt
  from store_sales
      ,date_dim
      ,customer
  where ss_sold_date_sk = d_date_sk
    and ss_item_sk = c_customer_sk
  group by substr(c_first_name, 1, 30),c_customer_sk,d_date, d_date_sk
  having count(*) >4) subq;

 -- group by with keys consisting of pk from multiple tables with extra expressions from both side
  explain cbo select c_customer_sk from
 (select substr(c_first_name, 1,30), c_customer_sk ,log2(d_date) solddate, d_date_sk, count(*) cnt
  from store_sales
      ,date_dim
      ,customer
  where ss_sold_date_sk = d_date_sk
    and ss_item_sk = c_customer_sk
  group by substr(c_first_name, 1, 30),c_customer_sk, log2(d_date), d_date_sk
  having count(*) >4) subq;


create table web_sales(ws_order_number int, ws_item_sk int, ws_price float,
    constraint pk1 primary key(ws_order_number, ws_item_sk) disable rely);
insert into web_sales values(1, 1, 1.2);
insert into web_sales values(1, 1, 1.2);
 explain cbo select count(distinct ws_order_number) from web_sales;
 select count(distinct ws_order_number) from web_sales;
 drop table web_sales;

create table t1(i int primary key disable rely, j int);
insert into t1 values(1,100),(2,200);
create table t2(i int primary key disable rely, j int);
insert into t2 values(2,1000),(4,500);

-- UNION
explain cbo select i from (select i, j from t1 union all select i,j from t2) subq group by i,j;
select i from (select i, j from t1 union all select i,j from t2) subq group by i,j;

-- INTERSECT
explain cbo select i from (select i, j from t1 intersect select i,j from t2) subq group by i,j;
select i from (select i, j from t1 intersect select i,j from t2) subq group by i,j;

drop table t1;
drop table t2;