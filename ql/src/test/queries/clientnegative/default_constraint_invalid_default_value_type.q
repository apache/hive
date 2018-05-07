-- year() isn't valid
create table t (i int, j string default cast(year("1970-01-01") as string));
