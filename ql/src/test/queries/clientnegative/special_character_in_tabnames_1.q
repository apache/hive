set hive.support.special.characters.tablename=false;

-- If hive.support.special.characters.tablename=false, we can not use special characters in table names.
-- The same query would work when it is set to true(default value).
-- Note that there is a positive test with the same name in clientpositive


create table `c/b/o_t1`(key string, value string, c_int int, c_float float, c_boolean boolean)  partitioned by (dt string) row format delimited fields terminated by ',' STORED AS TEXTFILE;





