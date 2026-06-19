CREATE TABLE l3_clarity__l3_snap_number_2018022300104(l3_snapshot_number bigint)
ROW FORMAT SERDE
'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS INPUTFORMAT
'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
;

CREATE TABLE l3_clarity__l3_monthly_dw_factplan_dw_stg_2018022300104_1(
  plan_detail_object_id bigint,
  project_object_id bigint,
  charge_code_object_id bigint,
  transclass_object_id bigint,
  resource_object_id bigint,
  slice_date varchar(50),
  split_amount varchar(50),
  split_units varchar(50),
  year_key varchar(20),
  quarter_key varchar(20),
  month_key varchar(50),
  week_key varchar(50),
  date_key varchar(50),
  fy_year_key varchar(50),
  fy_quarter_key string,
  fy_month_key string,
  supplier_object_id bigint,
  business_dept_object_id bigint,
  business_partner_percentage decimal(38,8))
ROW FORMAT SERDE
'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
;

CREATE TABLE l3_monthly_dw_dimplan(
  idp_warehouse_id bigint,
  idp_audit_id bigint,
  idp_data_date date,
  l3_snapshot_number bigint,
  plan_key bigint,
  project_key bigint,
  charge_code_key bigint,
  transclass_key bigint,
  resource_key bigint,
  finplan_detail_object_id bigint,
  project_object_id bigint,
  txn_class_object_id bigint,
  charge_code_object_id bigint,
  resoruce_object_id bigint,
  plan_name varchar(1500),
  plan_code varchar(500),
  plan_type varchar(50),
  period_type varchar(50),
  plan_description varchar(3000),
  plan_status varchar(50),
  period_start varchar(50),
  period_end varchar(50),
  plan_of_record varchar(1),
  percentage decimal(32,6),
  l3_created_date timestamp,
  bmo_cost_type varchar(30),
  bmo_fiscal_year varchar(50),
  clarity_updated_date timestamp,
  is_latest_snapshot bigint,
  latest_fiscal_budget_plan bigint,
  plan_category varchar(70),
  last_updated_by varchar(250))
CLUSTERED BY (
idp_data_date)
INTO 64 BUCKETS
ROW FORMAT SERDE
'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
;


CREATE TABLE l3_monthly_dw_dimplan_unbucketed(
  idp_warehouse_id bigint,
  idp_audit_id bigint,
  idp_data_date date,
  l3_snapshot_number bigint,
  plan_key bigint,
  project_key bigint,
  charge_code_key bigint,
  transclass_key bigint,
  resource_key bigint,
  finplan_detail_object_id bigint,
  project_object_id bigint,
  txn_class_object_id bigint,
  charge_code_object_id bigint,
  resoruce_object_id bigint,
  plan_name varchar(1500),
  plan_code varchar(500),
  plan_type varchar(50),
  period_type varchar(50),
  plan_description varchar(3000),
  plan_status varchar(50),
  period_start varchar(50),
  period_end varchar(50),
  plan_of_record varchar(1),
  percentage decimal(32,6),
  l3_created_date timestamp,
  bmo_cost_type varchar(30),
  bmo_fiscal_year varchar(50),
  clarity_updated_date timestamp,
  is_latest_snapshot bigint,
  latest_fiscal_budget_plan bigint,
  plan_category varchar(70),
  last_updated_by varchar(250))
ROW FORMAT SERDE
'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
;

CREATE TABLE l3_clarity__l3_monthly_dw_factplan_datajoin_1_s2_2018022300104_1(
  project_key bigint,
  l3_snapshot_number bigint,
  l3_created_date timestamp,
  project_object_id bigint,
  idp_data_date date)
ROW FORMAT SERDE
'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
;


load data local inpath '../../data/files/bucket_pruning/l3_clarity__l3_monthly_dw_factplan_datajoin_1_s2_2018022300104_1' into table l3_clarity__l3_monthly_dw_factplan_datajoin_1_s2_2018022300104_1;
load data local inpath '../../data/files/bucket_pruning/l3_clarity__l3_monthly_dw_factplan_dw_stg_2018022300104_1' into table l3_clarity__l3_monthly_dw_factplan_dw_stg_2018022300104_1;
load data local inpath '../../data/files/bucket_pruning/l3_clarity__l3_snap_number_2018022300104' into table l3_clarity__l3_snap_number_2018022300104;
load data local inpath '../../data/files/bucket_pruning/l3_monthly_dw_dimplan' into table l3_monthly_dw_dimplan_unbucketed;

INSERT OVERWRITE TABLE l3_monthly_dw_dimplan select * from l3_monthly_dw_dimplan_unbucketed;

analyze table l3_clarity__l3_monthly_dw_factplan_datajoin_1_s2_2018022300104_1 compute statistics;
analyze table l3_clarity__l3_monthly_dw_factplan_datajoin_1_s2_2018022300104_1 compute statistics for columns;

analyze table l3_clarity__l3_monthly_dw_factplan_dw_stg_2018022300104_1 compute statistics;
analyze table l3_clarity__l3_monthly_dw_factplan_dw_stg_2018022300104_1 compute statistics for columns;

analyze table l3_clarity__l3_snap_number_2018022300104 compute statistics;
analyze table l3_clarity__l3_snap_number_2018022300104 compute statistics for columns;

analyze table l3_monthly_dw_dimplan compute statistics;
analyze table l3_monthly_dw_dimplan compute statistics for columns;

set hive.explain.user=false;
set hive.auto.convert.join=true;
set hive.vectorized.execution.enabled=true;
set hive.auto.convert.join.noconditionaltask.size=200000000;
set hive.optimize.index.filter=true;

-- fixed bucket pruning off
set hive.tez.bucket.pruning=false;
EXPLAIN EXTENDED
SELECT DW.PROJECT_OBJECT_ID, S1.PLAN_KEY as PLAN_KEY, S2.PROJECT_KEY AS PROJECT_KEY
FROM l3_clarity__L3_SNAP_NUMBER_2018022300104 snap inner join
l3_clarity__L3_MONTHLY_DW_FACTPLAN_DW_STG_2018022300104_1 DW on 1=1
  LEFT OUTER JOIN L3_MONTHLY_DW_DIMPLAN S1
    ON S1.FINPLAN_DETAIL_OBJECT_ID = DW.PLAN_DETAIL_OBJECT_ID AND S1.L3_SNAPSHOT_NUMBER =snap.L3_snapshot_number
  AND S1.IDP_DATA_DATE = '2017-12-28'
  LEFT OUTER JOIN l3_clarity__L3_MONTHLY_DW_FACTPLAN_DATAJOIN_1_s2_2018022300104_1 S2
    ON S2.PROJECT_OBJECT_ID = DW.PROJECT_OBJECT_ID AND S2.L3_SNAPSHOT_NUMBER =snap.L3_snapshot_number
  AND S2.IDP_DATA_DATE = '2017-12-28'
where DW.PROJECT_OBJECT_ID =7147200
order by DW.PROJECT_OBJECT_ID, PLAN_KEY, PROJECT_KEY
limit 5;

SELECT DW.PROJECT_OBJECT_ID, S1.PLAN_KEY as PLAN_KEY, S2.PROJECT_KEY AS PROJECT_KEY
FROM l3_clarity__L3_SNAP_NUMBER_2018022300104 snap inner join
l3_clarity__L3_MONTHLY_DW_FACTPLAN_DW_STG_2018022300104_1 DW on 1=1
  LEFT OUTER JOIN L3_MONTHLY_DW_DIMPLAN S1
    ON S1.FINPLAN_DETAIL_OBJECT_ID = DW.PLAN_DETAIL_OBJECT_ID AND S1.L3_SNAPSHOT_NUMBER =snap.L3_snapshot_number
  AND S1.IDP_DATA_DATE = '2017-12-28'
  LEFT OUTER JOIN l3_clarity__L3_MONTHLY_DW_FACTPLAN_DATAJOIN_1_s2_2018022300104_1 S2
    ON S2.PROJECT_OBJECT_ID = DW.PROJECT_OBJECT_ID AND S2.L3_SNAPSHOT_NUMBER =snap.L3_snapshot_number
  AND S2.IDP_DATA_DATE = '2017-12-28'
where DW.PROJECT_OBJECT_ID =7147200
order by DW.PROJECT_OBJECT_ID, PLAN_KEY, PROJECT_KEY
limit 5;


-- fixed bucket pruning on
set hive.tez.bucket.pruning=true;

EXPLAIN EXTENDED
SELECT DW.PROJECT_OBJECT_ID, S1.PLAN_KEY as PLAN_KEY, S2.PROJECT_KEY AS PROJECT_KEY
FROM l3_clarity__L3_SNAP_NUMBER_2018022300104 snap inner join
l3_clarity__L3_MONTHLY_DW_FACTPLAN_DW_STG_2018022300104_1 DW on 1=1
  LEFT OUTER JOIN L3_MONTHLY_DW_DIMPLAN S1
    ON S1.FINPLAN_DETAIL_OBJECT_ID = DW.PLAN_DETAIL_OBJECT_ID AND S1.L3_SNAPSHOT_NUMBER =snap.L3_snapshot_number
  AND S1.IDP_DATA_DATE = '2017-12-28'
  LEFT OUTER JOIN l3_clarity__L3_MONTHLY_DW_FACTPLAN_DATAJOIN_1_s2_2018022300104_1 S2
    ON S2.PROJECT_OBJECT_ID = DW.PROJECT_OBJECT_ID AND S2.L3_SNAPSHOT_NUMBER =snap.L3_snapshot_number
  AND S2.IDP_DATA_DATE = '2017-12-28'
where DW.PROJECT_OBJECT_ID =7147200
order by DW.PROJECT_OBJECT_ID, PLAN_KEY, PROJECT_KEY
limit 5;

SELECT DW.PROJECT_OBJECT_ID, S1.PLAN_KEY as PLAN_KEY, S2.PROJECT_KEY AS PROJECT_KEY
FROM l3_clarity__L3_SNAP_NUMBER_2018022300104 snap inner join
l3_clarity__L3_MONTHLY_DW_FACTPLAN_DW_STG_2018022300104_1 DW on 1=1
  LEFT OUTER JOIN L3_MONTHLY_DW_DIMPLAN S1
    ON S1.FINPLAN_DETAIL_OBJECT_ID = DW.PLAN_DETAIL_OBJECT_ID AND S1.L3_SNAPSHOT_NUMBER =snap.L3_snapshot_number
  AND S1.IDP_DATA_DATE = '2017-12-28'
  LEFT OUTER JOIN l3_clarity__L3_MONTHLY_DW_FACTPLAN_DATAJOIN_1_s2_2018022300104_1 S2
    ON S2.PROJECT_OBJECT_ID = DW.PROJECT_OBJECT_ID AND S2.L3_SNAPSHOT_NUMBER =snap.L3_snapshot_number
  AND S2.IDP_DATA_DATE = '2017-12-28'
where DW.PROJECT_OBJECT_ID =7147200
order by DW.PROJECT_OBJECT_ID, PLAN_KEY, PROJECT_KEY
limit 5;

CREATE TABLE `test_table`( `col_1` int,`col_2` string,`col_3` string)
        CLUSTERED BY (col_1) INTO 4 BUCKETS;

insert into test_table values(1, 'one', 'ONE'), (2, 'two', 'TWO'), (3,'three','THREE'),(4,'four','FOUR');

select * from test_table;

explain extended select col_1, col_2, col_3 from test_table where col_1 <> 2 order by col_2;
select col_1, col_2, col_3 from test_table where col_1 <> 2 order by col_2;

explain extended select col_1, col_2, col_3 from test_table where col_1 = 2 order by col_2;
select col_1, col_2, col_3 from test_table where col_1 = 2 order by col_2;

drop table `test_table`;