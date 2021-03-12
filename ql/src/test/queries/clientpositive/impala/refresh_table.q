--! qt:dataset:impala_dataset

create external table external_tbl_pos (c1 int)
partitioned by (p_double double , p_boolean boolean, p_bigint bigint, p_float float, p_tinyint tinyint, p_smallint smallint, p_date date, p_char char(10), p_varchar varchar(10), p_str string);

explain
refresh `external_tbl_pos`;

explain
refresh `external_tbl_pos` partition(p_double=1.0, p_boolean=true, p_bigint=1, p_float=1.0, p_tinyint=1, p_smallint=1, p_date='2021-03-16', p_char="testChar", p_varchar="testVarchar", p_str="testString");