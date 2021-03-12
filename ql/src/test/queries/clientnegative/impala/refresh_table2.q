--! qt:dataset:impala_dataset

create external table external_tbl_neg (c1 int)
partitioned by (p_double double , p_boolean boolean, p_bigint bigint, p_float float, p_tinyint tinyint, p_smallint smallint, p_date date);

--! Refresh should fail for incomplete partition spec
explain
refresh `external_tbl_neg` partition(p_double=1.0, p_bigint=1);
