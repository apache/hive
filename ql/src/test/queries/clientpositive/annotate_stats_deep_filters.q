create table over1k_n4(
t tinyint,
si smallint,
i int,
b bigint,
f float,
d double,
bo boolean,
s string,
ts timestamp,
`dec` decimal(4,2),
bin binary)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

load data local inpath '../../data/files/over1k' overwrite into table over1k_n4;
load data local inpath '../../data/files/over1k' into table over1k_n4;

analyze table over1k_n4 compute statistics;
analyze table over1k_n4 compute statistics for columns;

set hive.stats.fetch.column.stats=true;
set hive.optimize.point.lookup=false;
explain select count(*) from over1k_n4 where (
(t=1 and si=2)
or (t=2 and si=3)
or (t=3 and si=4) 
or (t=4 and si=5) 
or (t=5 and si=6) 
or (t=6 and si=7) 
or (t=7 and si=8)
or (t=9 and si=10)
or (t=10 and si=11)
or (t=11 and si=12)
or (t=12 and si=13)
or (t=13 and si=14) 
or (t=14 and si=15) 
or (t=15 and si=16) 
or (t=16 and si=17) 
or (t=17 and si=18)
or (t=27 and si=28)
or (t=37 and si=38)
or (t=47 and si=48)
or (t=52 and si=53));

set hive.stats.fetch.column.stats=false;
explain select count(*) from over1k_n4 where (
(t=1 and si=2)
or (t=2 and si=3)
or (t=3 and si=4) 
or (t=4 and si=5) 
or (t=5 and si=6) 
or (t=6 and si=7) 
or (t=7 and si=8)
or (t=9 and si=10)
or (t=10 and si=11)
or (t=11 and si=12)
or (t=12 and si=13)
or (t=13 and si=14) 
or (t=14 and si=15) 
or (t=15 and si=16) 
or (t=16 and si=17) 
or (t=17 and si=18)
or (t=27 and si=28)
or (t=37 and si=38)
or (t=47 and si=48)
or (t=52 and si=53));
