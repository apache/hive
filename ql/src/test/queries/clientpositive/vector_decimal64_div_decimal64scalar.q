create external table vector_decimal64_div_decimal64scalar(ss_ext_list_price decimal(7,2), ss_ext_wholesale_cost decimal(7,2), ss_ext_discount_amt decimal(7,2), ss_ext_sales_price decimal(7,2)) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../../data/files/decimal64table.csv' OVERWRITE INTO TABLE vector_decimal64_div_decimal64scalar;
create table vector_decimal64_div_decimal64scalar_tmp(ss_ext_list_price decimal(7,2), ss_ext_wholesale_cost decimal(7,2), ss_ext_discount_amt decimal(7,2), ss_ext_sales_price decimal(7,2)) stored as ORC;
insert into table vector_decimal64_div_decimal64scalar_tmp select * from vector_decimal64_div_decimal64scalar;
explain vectorization detail select sum(((ss_ext_list_price-ss_ext_wholesale_cost-ss_ext_discount_amt)+ss_ext_sales_price)/2.268767) from vector_decimal64_div_decimal64scalar_tmp;
select sum(((ss_ext_list_price-ss_ext_wholesale_cost-ss_ext_discount_amt)+ss_ext_sales_price)/2.268767) from vector_decimal64_div_decimal64scalar_tmp;
explain vectorization detail select sum(((ss_ext_list_price-ss_ext_wholesale_cost-ss_ext_discount_amt)+ss_ext_sales_price)/2.2) from vector_decimal64_div_decimal64scalar_tmp;
select sum(((ss_ext_list_price-ss_ext_wholesale_cost-ss_ext_discount_amt)+ss_ext_sales_price)/2.2) from vector_decimal64_div_decimal64scalar_tmp;
