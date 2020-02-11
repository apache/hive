

CREATE TABLE table1_n2 (
       a STRING,
       b ARRAY<STRING>,
       c ARRAY<MAP<STRING,STRING>>,
       d MAP<STRING,ARRAY<STRING>>
       ) STORED AS TEXTFILE;
DESCRIBE table1_n2;
DESCRIBE EXTENDED table1_n2;

LOAD DATA LOCAL INPATH '../../data/files/create_nested_type.txt' OVERWRITE INTO TABLE table1_n2;

SELECT * from table1_n2;


