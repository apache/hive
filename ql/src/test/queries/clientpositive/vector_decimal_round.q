set hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=minimal;

create table decimal_tbl_txt (dec decimal(10,0)) 
ROW FORMAT DELIMITED
   FIELDS TERMINATED BY ' '
STORED AS TEXTFILE;

insert into table decimal_tbl_txt values(101);

select * from decimal_tbl_txt;

explain
select dec, round(dec, -1) from decimal_tbl_txt order by dec;

select dec, round(dec, -1) from decimal_tbl_txt order by dec;

explain
select dec, round(dec, -1) from decimal_tbl_txt order by round(dec, -1);

select dec, round(dec, -1) from decimal_tbl_txt order by round(dec, -1);

create table decimal_tbl_rc (dec decimal(10,0))
row format serde 'org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe' stored as rcfile;

insert into table decimal_tbl_rc values(101);

select * from decimal_tbl_rc;

explain
select dec, round(dec, -1) from decimal_tbl_rc order by dec;

select dec, round(dec, -1) from decimal_tbl_rc order by dec;

explain
select dec, round(dec, -1) from decimal_tbl_rc order by round(dec, -1);

select dec, round(dec, -1) from decimal_tbl_rc order by round(dec, -1);

create table decimal_tbl_orc (dec decimal(10,0))
stored as orc;

insert into table decimal_tbl_orc values(101);

select * from decimal_tbl_orc;

explain
select dec, round(dec, -1) from decimal_tbl_orc order by dec;

select dec, round(dec, -1) from decimal_tbl_orc order by dec;

explain
select dec, round(dec, -1) from decimal_tbl_orc order by round(dec, -1);

select dec, round(dec, -1) from decimal_tbl_orc order by round(dec, -1);