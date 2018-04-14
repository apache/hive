set hive.cbo.enable=true;

create table cbo_t3(key string, value string, c_int int, c_float float, c_boolean boolean)  row format delimited fields terminated by ',' STORED AS TEXTFILE;

load data local inpath '${hiveconf:test.data.dir}/cbo_t3.txt' into table cbo_t3;

analyze table cbo_t3 compute statistics;
analyze table cbo_t3 compute statistics for columns key, value, c_int, c_float, c_boolean;
