set hive.cbo.enable=true;

create table cbo_t2(key string, value string, c_int int, c_float float, c_boolean boolean)  partitioned by (dt string) row format delimited fields terminated by ',' STORED AS TEXTFILE;

load data local inpath '${hiveconf:test.data.dir}/cbo_t2.txt' into table cbo_t2 partition (dt='2014');

analyze table cbo_t2 partition (dt) compute statistics;
analyze table cbo_t2 compute statistics for columns key, value, c_int, c_float, c_boolean;
