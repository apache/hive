--! qt:dataset:src1
--! qt:dataset:src
-- SORT_QUERY_RESULTS

explain
select * from src a join src1 b on a.key = b.key;

select * from src a join src1 b on a.key = b.key;

explain
select * from
(select * from src where 1 = 0)a
join
(select * from src1)b on a.key = b.key;

select * from
(select * from src where 1 = 0)a
join
(select * from src1)b on a.key = b.key;

explain
select * from
(select * from src where 1 = 0)a
left outer join
(select * from src1)b on a.key = b.key;

select * from
(select * from src where 1 = 0)a
left outer join
(select * from src1)b on a.key = b.key;

explain
select * from
(select * from src where 1 = 0)a
right outer join
(select * from src1)b on a.key = b.key;

select * from
(select * from src where 1 = 0)a
right outer join
(select * from src1)b on a.key = b.key;

explain
select * from
(select * from src where 1 = 0)a
full outer join
(select * from src1)b on a.key = b.key;

select * from
(select * from src where 1 = 0)a
full outer join
(select * from src1)b on a.key = b.key;

explain
select * from
(select * from src)a
right outer join
(select * from src1 where 1 = 0)b on a.key = b.key;

select * from
(select * from src)a
right outer join
(select * from src1 where 1 = 0)b on a.key = b.key;

explain select * from src join src1 on src.key = src1.key and src.value = src1.value
    where 4 between src.key and src.value;

    CREATE TABLE `table1_n8`(
       `idp_warehouse_id` bigint,
       `idp_audit_id` bigint,
       `idp_effective_date` date,
       `idp_end_date` date,
       `idp_delete_date` date,
       `pruid` varchar(32),
       `prid` bigint,
       `prtimesheetid` bigint,
       `prassignmentid` bigint,
       `prchargecodeid` bigint,
       `prtypecodeid` bigint,
       `prsequence` bigint,
       `prmodby` varchar(96),
       `prmodtime` timestamp,
       `prrmexported` bigint,
       `prrmckdel` bigint,
       `slice_status` int,
       `role_id` bigint,
       `user_lov1` varchar(30),
       `user_lov2` varchar(30),
       `incident_id` bigint,
       `incident_investment_id` bigint,
       `odf_ss_actuals` bigint,
       `practsum` decimal(38,20));

    CREATE TABLE `table2_n4`(
       `idp_warehouse_id` bigint,
       `idp_audit_id` bigint,
       `idp_effective_date` date,
       `idp_end_date` date,
       `idp_delete_date` date,
       `pruid` varchar(32),
       `prid` bigint,
       `prtimesheetid` bigint,
       `prassignmentid` bigint,
       `prchargecodeid` bigint,
       `prtypecodeid` bigint,
       `prsequence` bigint,
       `prmodby` varchar(96),
       `prmodtime` timestamp,
       `prrmexported` bigint,
       `prrmckdel` bigint,
       `slice_status` int,
       `role_id` bigint,
       `user_lov1` varchar(30),
       `user_lov2` varchar(30),
       `incident_id` bigint,
       `incident_investment_id` bigint,
       `odf_ss_actuals` bigint,
       `practsum` decimal(38,20));

    explain SELECT          s.idp_warehouse_id AS source_warehouse_id
    FROM            table1_n8 s
    JOIN

                           table2_n4 d
    ON              (
                                    s.prid = d.prid )
    JOIN
                             table2_n4 e
    ON
                                    s.prid = e.prid
    WHERE
    concat(
                    CASE
                                    WHEN s.prid IS NULL THEN 1
                                    ELSE s.prid
                    END,',',
                    CASE
                                    WHEN s.prtimesheetid IS NULL THEN 1
                                    ELSE s.prtimesheetid
                    END,',',
                    CASE
                                    WHEN s.prassignmentid IS NULL THEN 1
                                    ELSE s.prassignmentid
                    END,',',
                    CASE
                                    WHEN s.prchargecodeid IS NULL THEN 1
                                    ELSE s.prchargecodeid
                    END,',',
                    CASE
                                    WHEN (s.prtypecodeid) IS NULL THEN ''
                                    ELSE s.prtypecodeid
                    END,',',
                    CASE
                                    WHEN s.practsum IS NULL THEN 1
                                    ELSE s.practsum
                    END,',',
                    CASE
                                    WHEN s.prsequence IS NULL THEN 1
                                    ELSE s.prsequence
                    END,',',
                    CASE
                                    WHEN length(s.prmodby) IS NULL THEN ''
                                    ELSE s.prmodby
                    END,',',
                    CASE
                                    WHEN s.prmodtime IS NULL THEN cast(from_unixtime(unix_timestamp('2017-12-08','yyyy-MM-dd') ) AS timestamp)
                                    ELSE s.prmodtime
                    END,',',
                    CASE
                                    WHEN s.prrmexported IS NULL THEN 1
                                    ELSE s.prrmexported
                    END,',',
                    CASE
                                    WHEN s.prrmckdel IS NULL THEN 1
                                    ELSE s.prrmckdel
                    END,',',
                    CASE
                                    WHEN s.slice_status IS NULL THEN 1
                                    ELSE s.slice_status
                    END,',',
                    CASE
                                    WHEN s.role_id IS NULL THEN 1
                                    ELSE s.role_id
                    END,',',
                    CASE
                                    WHEN length(s.user_lov1) IS NULL THEN ''
                                    ELSE s.user_lov1
                    END,',',
                    CASE
                                    WHEN length(s.user_lov2) IS NULL THEN ''
                                    ELSE s.user_lov2
                    END,',',
                    CASE
                                    WHEN s.incident_id IS NULL THEN 1
                                    ELSE s.incident_id
                    END,',',
                    CASE
                                    WHEN s.incident_investment_id IS NULL THEN 1
                                    ELSE s.incident_investment_id
                    END,',',
                    CASE
                                    WHEN s.odf_ss_actuals IS NULL THEN 1
                                    ELSE s.odf_ss_actuals
                    END ) != concat(
                    CASE
                                    WHEN length(d.pruid) IS NULL THEN ''
                                    ELSE d.pruid
                    END,',',
                    CASE
                                    WHEN d.prid IS NULL THEN 1
                                    ELSE d.prid
                    END,',',
                    CASE
                                    WHEN d.prtimesheetid IS NULL THEN 1
                                    ELSE d.prtimesheetid
                    END,',',
                    CASE
                                    WHEN d.prassignmentid IS NULL THEN 1
                                    ELSE d.prassignmentid
                    END,',',
                    CASE
                                    WHEN d.prchargecodeid IS NULL THEN 1
                                    ELSE d.prchargecodeid
                    END,',',
                    CASE
                                    WHEN (d.prtypecodeid) IS NULL THEN ''
                                    ELSE d.prtypecodeid
                    END,',',
                    CASE
                                    WHEN d.practsum IS NULL THEN 1
                                    ELSE d.practsum
                    END,',',
                    CASE
                                    WHEN d.prsequence IS NULL THEN 1
                                    ELSE d.prsequence
                    END,',',
                    CASE
                                    WHEN length(d.prmodby) IS NULL THEN ''
                                    ELSE d.prmodby
                    END,',',
                    CASE
                                    WHEN d.prmodtime IS NULL THEN cast(from_unixtime(unix_timestamp('2017-12-08','yyyy-MM-dd') ) AS timestamp)
                                    ELSE d.prmodtime
                    END,',',
                    CASE
                                    WHEN d.prrmexported IS NULL THEN 1
                                    ELSE d.prrmexported
                    END,',',
                    CASE
                                    WHEN d.prrmckdel IS NULL THEN 1
                                    ELSE d.prrmckdel
                    END,',',
                    CASE
                                    WHEN d.slice_status IS NULL THEN 1
                                    ELSE d.slice_status
                    END,',',
                    CASE
                                    WHEN d.role_id IS NULL THEN 1
                                    ELSE d.role_id
                    END,',',
                    CASE
                                    WHEN length(d.user_lov1) IS NULL THEN ''
                                    ELSE d.user_lov1
                    END,',',
                    CASE
                                    WHEN length(d.user_lov2) IS NULL THEN ''
                                    ELSE d.user_lov2
                    END,',',
                    CASE
                                    WHEN d.incident_id IS NULL THEN 1
                                    ELSE d.incident_id
                    END,',',
                    CASE
                                    WHEN d.incident_investment_id IS NULL THEN 1
                                    ELSE d.incident_investment_id
                    END,',',
                    CASE
                                    WHEN d.odf_ss_actuals IS NULL THEN 1
                                    ELSE d.odf_ss_actuals
                    END );

drop table table2_n4;
drop table table1_n8;


