create external table vector_decimal64_mul_intcolumn(ss_ext_list_price decimal(7,2), ss_ext_wholesale_cost decimal(19,1), ss_ext_discount_amt int, ss_ext_sales_price decimal(7,2)) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../../data/files/decimal64table.csv' OVERWRITE INTO TABLE vector_decimal64_mul_intcolumn;
create table vector_decimal64_mul_intcolumn_tmp(ss_ext_list_price decimal(7,2), ss_ext_wholesale_cost decimal(19,1), ss_ext_discount_amt int, ss_ext_sales_price decimal(7,2)) stored as ORC;
insert into table vector_decimal64_mul_intcolumn_tmp select * from vector_decimal64_mul_intcolumn;
explain vectorization detail select sum(ss_ext_list_price*ss_ext_discount_amt) from vector_decimal64_mul_intcolumn_tmp;
select sum(ss_ext_list_price*ss_ext_discount_amt) from vector_decimal64_mul_intcolumn_tmp;
explain vectorization detail select sum(ss_ext_wholesale_cost*ss_ext_discount_amt) from vector_decimal64_mul_intcolumn_tmp;
select sum(ss_ext_wholesale_cost*ss_ext_discount_amt) from vector_decimal64_mul_intcolumn_tmp;
