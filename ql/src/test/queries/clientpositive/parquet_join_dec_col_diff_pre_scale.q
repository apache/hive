CREATE TABLE table_a (col_dec_a decimal(12,7));
CREATE TABLE table_b(col_dec_b decimal(15,5));
INSERT INTO table_a VALUES (12345.6789101);
INSERT INTO table_b VALUES (1234567891.01112);

set hive.default.fileformat=parquet;

explain create table target as
select table_a.col_dec_a target_col
from table_a
left outer join table_b on
table_a.col_dec_a = table_b.col_dec_b;

create table target as
select table_a.col_dec_a target_col
from table_a
left outer join table_b on
table_a.col_dec_a = table_b.col_dec_b;

desc target;
select * from target;