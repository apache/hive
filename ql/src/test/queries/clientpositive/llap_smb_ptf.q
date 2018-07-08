set hive.exec.dynamic.partition.mode=nonstrict;

CREATE TABLE cogs_alc_rqst_trgt_offs(
  cogs_alc_rqst_trgt_offs_id int,
  last_upd_sysusr_id string,
  last_upd_ts string,
  cogs_alc_rqst_id int,
  offs_mnr_acct_nbr smallint,
  offs_mjr_acct_nbr smallint,
  offs_amt decimal(17,4),
  offs_dr_cr_ind string,
  offs_loc_nbr string,
  offs_loc_typ_cd string,
  offs_sap_co_nbr string)
PARTITIONED BY (
  part_dt string)
CLUSTERED BY (cogs_alc_rqst_id)
SORTED BY (cogs_alc_rqst_id)
INTO 5 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS ORC
TBLPROPERTIES (
'orc.compress'='SNAPPY');

CREATE TABLE cogs_alc_rqst(
  cogs_alc_rqst_id int,
  crt_sysusr_id string,
  crt_ts string,
  last_upd_sysusr_id string,
  last_upd_ts string,
  cogs_alc_bth_prcss_id int,
  cogs_alc_mde_cd smallint,
  cogs_alc_stat_cd smallint,
  cogs_alc_typ_cd smallint,
  cogs_alc_basis_cd smallint,
  fin_post_typ_cd smallint,
  eff_bgn_dt string,
  eff_end_dt string,
  cogs_alc_pstruct_cd smallint,
  cogs_alc_os_cd smallint,
  cogs_alc_fti_cd smallint,
  cogs_alc_os_fti_cd smallint,
  cogs_alc_rqst_dt string,
  bgn_fscl_yr smallint,
  bgn_fscl_wk_nbr smallint,
  bgn_fscl_prd_nbr smallint,
  bgn_dt string,
  end_fscl_yr smallint,
  end_fscl_wk_nbr smallint,
  end_fscl_prd_nbr smallint,
  end_dt string,
  alloc_amt decimal(17,4),
  dr_cr_ind string,
  alloc_pvndr_nbr int,
  alloc_mvndr_nbr int,
  purch_vndr_typ_ind string,
  alloc_mjr_acct_nbr smallint,
  alloc_mnr_acct_nbr smallint,
  cogs_alc_prnt_rqst_id int,
  cogs_alc_prnt_rqst_dt string,
  sap_xref_txt string,
  stats_xref_txt string,
  alloc_stat_dest_ind string,
  invc_nbr string,
  ap_po_nbr string,
  bth_src_file_line_nbr int,
  cogs_alc_bth_src_xref_id string,
  mer_alloc_flg string,
  sap_snd_flg string)
PARTITIONED BY (
  part_dt string)
CLUSTERED BY (cogs_alc_rqst_id)
SORTED BY (cogs_alc_rqst_id)
INTO 5 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS ORC
TBLPROPERTIES (
'orc.compress'='SNAPPY',
'totalSize'='820240');

CREATE TABLE cogs_alc_stat(
  cogs_alc_bth_prcss_id int,
  cogs_alc_rqst_id int,
  cogs_alc_stat_cd smallint,
  last_upd_pgm_id string,
  last_upd_ts string,
  d_stat_cd string,
  intrvl_cnt int)
PARTITIONED BY (
  part_stat_desc string)
CLUSTERED BY (cogs_alc_rqst_id)
SORTED BY (cogs_alc_rqst_id)
INTO 5 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS ORC;


CREATE TABLE int_cogs_alc_post_stg(
  cogs_alc_rqst_id int,
  cogs_alc_rqst_dt string,
  loc_nbr string,
  loc_typ_cd string,
  mvndr_nbr int,
  mer_dept_nbr smallint,
  sku_nbr int,
  last_upd_pgm_id string,
  last_upd_ts string,
  cogs_alc_bth_prcss_id int,
  alloc_assg_ind string,
  alloc_stat_dest_ind string,
  bgn_dt string,
  end_dt string,
  pvndr_nbr int,
  ibu_id string,
  ext_cost_amt decimal(22,9),
  ext_cost_rnd_amt decimal(17,4),
  ext_retl_amt decimal(22,9),
  ext_retl_rnd_amt decimal(17,4),
  alloc_mjr_acct_nbr smallint,
  alloc_mnr_acct_nbr smallint,
  recpt_typ_cd string,
  recpt_sub_typ_cd string,
  onln_rlse_typ_ind string,
  rcvd_unt_cnt int,
  ord_unt_qty int,
  purch_typ_ind string,
  keyrec_typ_ind string,
  key_xfer_nbr int,
  dtl_rcvd_dt string,
  po_nbr string,
  invc_nbr string,
  invc_dt string,
  pj_trans_typ_cd string,
  src_sub_sys_cd string,
  fin_sys_adoc_nbr string,
  rga_txt string,
  rtv_evnt_txt string,
  rtv_evnt_ts string,
  stk_flow_thru_ind string,
  po_crt_dt string,
  upc_cd string,
  fin_post_typ_cd smallint,
  offs_flg string,
  sap_co_nbr string,
  cost_ctr_id string,
  cogs_alc_stat_cd smallint,
  acct_typ_ind string,
  dom_purch_inv_ind string)
PARTITIONED BY (
  part_dt string)
CLUSTERED BY (cogs_alc_rqst_id)
SORTED BY (cogs_alc_rqst_id)
INTO 5 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS ORC
TBLPROPERTIES (
'orc.compress'='SNAPPY');

set hive.enforce.sortmergebucketmapjoin=false;
set hive.optimize.bucketmapjoin=true;
set hive.optimize.bucketmapjoin.sortedmerge=true;
set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask.size=1;
set hive.merge.nway.joins=false;

set hive.auto.convert.sortmerge.join=true;

-- Should NOT create SMB
EXPLAIN
SELECT status_rqst.*
FROM (
  SELECT status_rnk.cogs_alc_rqst_id,
         rqst.fin_post_typ_cd,
         rqst.dr_cr_ind,
         rqst.cogs_alc_typ_cd,
         rqst.mer_alloc_flg,
         rqst.cogs_alc_basis_cd,
         rqst.end_dt,
         offs_trgt.offs_mnr_acct_nbr,
         offs_trgt.offs_mjr_acct_nbr,
         offs_trgt.offs_dr_cr_ind,
         offs_trgt.offs_sap_co_nbr,
         offs_trgt.offs_loc_nbr,
         '201611160940'
  FROM (
    SELECT distinct cogs_alc_rqst_id,
                    last_upd_ts AS rnk
    FROM COGS_ALC_STAT ) status_rnk
    JOIN (
      SELECT fin_post_typ_cd,
             dr_cr_ind,
             cogs_alc_typ_cd,
             mer_alloc_flg,
             cogs_alc_rqst_id,
             cogs_alc_rqst_dt,
             cogs_alc_basis_cd,
             end_dt,
             Row_number( )
             over (
             PARTITION BY cogs_alc_rqst_id, last_upd_ts
             ORDER BY last_upd_ts  ) AS rnk
      FROM COGS_ALC_RQST ) rqst
      ON ( rqst.cogs_alc_rqst_id = status_rnk.cogs_alc_rqst_id )
    LEFT OUTER JOIN (
      SELECT OFF.*
      FROM (
        SELECT offs_mnr_acct_nbr,
               offs_mjr_acct_nbr,
               offs_loc_nbr,
               offs_dr_cr_ind,
               offs_sap_co_nbr,
               cogs_alc_rqst_id,
               Row_number( )
               over (
               PARTITION BY cogs_alc_rqst_id, last_upd_ts
               ORDER BY last_upd_ts  ) AS rnk
        FROM COGS_ALC_RQST_TRGT_OFFS ) OFF
      WHERE OFF.rnk = 1 ) offs_trgt
      ON ( rqst.cogs_alc_rqst_id = offs_trgt.cogs_alc_rqst_id )
  WHERE rqst.rnk = 1 ) status_rqst
  LEFT OUTER JOIN (
    SELECT DISTINCT temp_post.cogs_alc_rqst_id,
                    temp_post.last_upd_ts
    FROM INT_COGS_ALC_POST_STG temp_post
    WHERE part_dt IN ( '201611181320' ) ) failed_rqst
    ON ( failed_rqst.cogs_alc_rqst_id = status_rqst.cogs_alc_rqst_id )
WHERE failed_rqst.cogs_alc_rqst_id IS NULL;

