--! qt:dataset:src
create table alter_bucket_change_col_t1(Serial_Num string, value string) partitioned by (ds string) clustered by (Serial_Num) into 10 buckets;

describe formatted alter_bucket_change_col_t1;

-- Test changing name of bucket column

alter table alter_bucket_change_col_t1 change Serial_num Key string;

describe formatted alter_bucket_change_col_t1;