create external table vector_decimal64_mul_decimal64column(ss_ext_list_price decimal(7,2), ss_ext_wholesale_cost decimal(7,1), ss_ext_discount_amt decimal(7,2), ss_ext_sales_price decimal(7,2)) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../../data/files/decimal64table.csv' OVERWRITE INTO TABLE vector_decimal64_mul_decimal64column;
create table vector_decimal64_mul_decimal64column_tmp(ss_ext_list_price decimal(7,2), ss_ext_wholesale_cost decimal(7,1), ss_ext_discount_amt decimal(7,2), ss_ext_sales_price decimal(7,2)) stored as ORC;
insert into table vector_decimal64_mul_decimal64column_tmp select * from vector_decimal64_mul_decimal64column;
explain vectorization detail select sum(ss_ext_list_price+ss_ext_wholesale_cost) from vector_decimal64_mul_decimal64column_tmp;
select sum(ss_ext_list_price+ss_ext_wholesale_cost) from vector_decimal64_mul_decimal64column_tmp;
