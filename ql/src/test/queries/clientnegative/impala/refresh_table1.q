--! qt:dataset:impala_dataset

create table managed_tbl_neg (c1 int)
partitioned by (p_double double , p_boolean boolean, p_bigint bigint, p_float float, p_tinyint tinyint, p_smallint smallint, p_date date);

--! Refresh should fail for managed table
explain
refresh `managed_tbl_neg`;
