-- with string
CREATE TABLE escape3_1
( 
GERUND STRING, 
ABBREV STRING,
CODE SMALLINT 
) 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' ESCAPED BY '\134' 
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/data_with_escape.txt' INTO TABLE escape3_1;

select * from escape3_1;

-- with varchar
CREATE TABLE escape3_2
( 
GERUND VARCHAR(10), 
ABBREV VARCHAR(3), 
CODE SMALLINT 
) 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' ESCAPED BY '\134' 
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/data_with_escape.txt' INTO TABLE escape3_2;

select * from escape3_2;

-- with char
CREATE TABLE escape3_3
( 
GERUND CHAR(10), 
ABBREV CHAR(3), 
CODE SMALLINT 
) 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|' ESCAPED BY '\134' 
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/data_with_escape.txt' INTO TABLE escape3_3;

select * from escape3_3;

DROP TABLE escape3_1;
DROP TABLE escape3_2;
DROP TABLE escape3_3;
