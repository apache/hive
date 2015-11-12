CREATE TABLE IF NOT EXISTS bucketinput( 
data string 
) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
CREATE TABLE IF NOT EXISTS bucketoutput1( 
data string 
)CLUSTERED BY(data) 
INTO 2 BUCKETS 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
CREATE TABLE IF NOT EXISTS bucketoutput2( 
data string 
)CLUSTERED BY(data) 
INTO 2 BUCKETS 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
insert into table bucketinput values ("firstinsert1");
insert into table bucketinput values ("firstinsert2");
insert into table bucketinput values ("firstinsert3");
set hive.enforce.bucketing = true; 
set hive.enforce.sorting=true;
insert overwrite table bucketoutput1 select * from bucketinput where data like 'first%'; 
CREATE TABLE temp1
(
    change string,
    num string
)
CLUSTERED BY (num) SORTED BY (num) INTO 4 BUCKETS;
explain insert overwrite table temp1 select data, data from bucketinput;
CREATE TABLE temp2
(
    create_ts STRING ,
    change STRING,
    num STRING
)
CLUSTERED BY (create_ts) SORTED BY (num) INTO 4 BUCKETS;

explain
INSERT OVERWRITE TABLE temp2
SELECT change, change,num
FROM temp1;
set hive.auto.convert.sortmerge.join=true; 
set hive.optimize.bucketmapjoin = true; 
set hive.optimize.bucketmapjoin.sortedmerge = true; 
select * from bucketoutput1 a join bucketoutput2 b on (a.data=b.data);
drop table temp1;
drop table temp2;
drop table buckettestinput;
drop table buckettestoutput1;
drop table buckettestoutput2;

