--! qt:dataset:src
SET hive.vectorized.execution.enabled=false;
SET hive.auto.convert.join=true;
SET hive.auto.convert.join.noconditionaltask=true;
SET hive.auto.convert.join.noconditionaltask.size=1000000000;
set hive.mapjoin.optimized.hashtable=true;
set hive.mapred.mode=nonstrict;
create table service_request_clean(
cnctevn_id          	string              ,
svcrqst_id          	string              ,
svcrqst_crt_dts     	string              ,
subject_seq_no      	int                 ,
plan_component      	string              ,
cust_segment        	string              ,
cnctyp_cd           	string              ,
cnctmd_cd           	string              ,
cnctevs_cd          	string              ,
svcrtyp_cd          	string              ,
svrstyp_cd          	string              ,
cmpltyp_cd          	string              ,
catsrsn_cd          	string              ,
apealvl_cd          	string              ,
cnstnty_cd          	string              ,
svcrqst_asrqst_ind  	string              ,
svcrqst_rtnorig_in  	string              ,
svcrqst_vwasof_dt   	string              ,
sum_reason_cd       	string              ,
sum_reason          	string              ,
crsr_master_claim_index	string              ,
svcrqct_cds         	array<string>       ,
svcrqst_lupdt       	string              ,
crsr_lupdt          	timestamp           ,
cntevsds_lupdt      	string              ,
ignore_me           	int                 ,
notes               	array<string>       )
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY  '\t' LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;

load data local inpath '../../data/files/service_request_clean.txt' into table service_request_clean;

create table ct_events_clean(
contact_event_id    	string              ,
ce_create_dt        	string              ,
ce_end_dt           	string              ,
contact_type        	string              ,
cnctevs_cd          	string              ,
contact_mode        	string              ,
cntvnst_stts_cd     	string              ,
total_transfers     	int                 ,
ce_notes            	array<string>       )
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY  '\t' LINES TERMINATED BY '\n' 
STORED AS TEXTFILE;

load data local inpath '../../data/files/ct_events_clean.txt' into table ct_events_clean;

set hive.mapjoin.hybridgrace.hashtable=false;
drop table if exists ct_events1_test;

explain extended
create table ct_events1_test
as select  a.*,
b.svcrqst_id,
b.svcrqct_cds,
b.svcrtyp_cd,
b.cmpltyp_cd,
b.sum_reason_cd as src,
b.cnctmd_cd,
b.notes
from ct_events_clean a
inner join
service_request_clean b
on a.contact_event_id = b.cnctevn_id;

-- SORT_QUERY_RESULTS

create table ct_events1_test
as select  a.*,
b.svcrqst_id,
b.svcrqct_cds,
b.svcrtyp_cd,
b.cmpltyp_cd,
b.sum_reason_cd as src,
b.cnctmd_cd,
b.notes
from ct_events_clean a
inner join
service_request_clean b
on a.contact_event_id = b.cnctevn_id;

select * from ct_events1_test;

set hive.mapjoin.hybridgrace.hashtable=true;
drop table if exists ct_events1_test;

explain extended
create table ct_events1_test
as select  a.*,
b.svcrqst_id,
b.svcrqct_cds,
b.svcrtyp_cd,
b.cmpltyp_cd,
b.sum_reason_cd as src,
b.cnctmd_cd,
b.notes
from ct_events_clean a
inner join
service_request_clean b
on a.contact_event_id = b.cnctevn_id;

-- SORT_QUERY_RESULTS

create table ct_events1_test
as select  a.*,
b.svcrqst_id,
b.svcrqct_cds,
b.svcrtyp_cd,
b.cmpltyp_cd,
b.sum_reason_cd as src,
b.cnctmd_cd,
b.notes
from ct_events_clean a
inner join
service_request_clean b
on a.contact_event_id = b.cnctevn_id;

select * from ct_events1_test;













