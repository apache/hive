create external table cdpd4378(ss_ext_list_price decimal(7,2), ss_ext_wholesale_cost decimal(7,2), ss_ext_discount_amt decimal(7,2), ss_ext_sales_price decimal(7,2)) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../../data/files/decimal64table.csv' OVERWRITE INTO TABLE cdpd4378;
create table cdpd4378_tmp(ss_ext_list_price decimal(7,2), ss_ext_wholesale_cost decimal(7,2), ss_ext_discount_amt decimal(7,2), ss_ext_sales_price decimal(7,2)) stored as ORC;
insert into table cdpd4378_tmp select * from cdpd4378;
explain vectorization detail select sum(((ss_ext_list_price-ss_ext_wholesale_cost-ss_ext_discount_amt)+ss_ext_sales_price)/2.268767) from cdpd4378_tmp;
select sum(((ss_ext_list_price-ss_ext_wholesale_cost-ss_ext_discount_amt)+ss_ext_sales_price)/2.268767) from cdpd4378_tmp;
explain vectorization detail select sum(((ss_ext_list_price-ss_ext_wholesale_cost-ss_ext_discount_amt)+ss_ext_sales_price)/2.2) from cdpd4378_tmp;
select sum(((ss_ext_list_price-ss_ext_wholesale_cost-ss_ext_discount_amt)+ss_ext_sales_price)/2.2) from cdpd4378_tmp;
