CREATE TABLE mytable_n2(key binary, value int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '9'
STORED AS TEXTFILE;
-- this query loads native binary data, stores in a table and then queries it. Note that string.txt contains binary data. Also uses transform clause and then length udf.

LOAD DATA LOCAL INPATH '../../data/files/string.txt' INTO TABLE mytable_n2;

create table dest1_n155 (key binary, value int);

insert overwrite table dest1_n155 select transform(*) using 'cat' as key binary, value int from mytable_n2; 

select key, value, length (key) from dest1_n155;
