set hive.optimize.constant.propagation=true;

CREATE TABLE dest1(d date, t timestamp);

EXPLAIN
INSERT OVERWRITE TABLE dest1
SELECT cast('2013-11-17' as date), cast(cast('1.3041352164485E9' as double) as timestamp)
       FROM src tablesample (1 rows);

INSERT OVERWRITE TABLE dest1
SELECT cast('2013-11-17' as date), cast(cast('1.3041352164485E9' as double) as timestamp)
       FROM src tablesample (1 rows);

SELECT * FROM dest1;

SELECT key, value FROM src WHERE key = cast(86 as double);

CREATE TABLE primitives1 (
  id INT  ,
  bool_col BOOLEAN  ,
  tinyint_col TINYINT  ,
  smallint_col SMALLINT  ,
  int_col INT  ,
  bigint_col BIGINT  ,
  float_col FLOAT  ,
  double_col DOUBLE  ,
  date_string_col STRING  ,
  string_col STRING  ,
  timestamp_col TIMESTAMP  )
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
  ESCAPED BY '\\'
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/types/primitives/090101.txt'
OVERWRITE INTO TABLE primitives1 ;


select id,bool_col,tinyint_col,smallint_col,int_col,bigint_col,float_col,double_col from primitives1 where id = cast (0 as float) and bool_col = cast('true' as boolean) and tinyint_col = cast(0 as double) and smallint_col = cast(0 as bigint) and int_col = cast (0 as double) and bigint_col = cast(0 as tinyint) and float_col = cast(0.0 as string) and  double_col = cast (0.0 as float);
