--! qt:replace:/(\t)[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/$1#Masked#/
-- Repro for: java.lang.ClassCastException: LongWritable cannot be cast to ShortWritable
--            (surfaces as "Hive Runtime Error while closing operators" here;
--            as "Error evaluating if(_colN is not null, _colN, 0L)" in the
--            original prod trace -- same underlying Writable type mismatch)
--            on a Reducer vertex (dynamic partition hash join).
--
-- Trigger conditions, isolated empirically (no external jar involved):
--   1. A base table with a physical SMALLINT column (flag_cnt).
--   2. That same base table scanned multiple times in nested subqueries
--      feeding a LEFT OUTER JOIN nested inside a FULL OUTER JOIN.
--   3. A subquery re-derives a value under the SAME alias name (flag_cnt)
--      via count(1), which is BIGINT -- COALESCE(x.flag_cnt, 0) at the top
--      collides the two typeinfos across the join's null-padding path.
--   4. A CREATE TEMPORARY FUNCTION wrapping a non-deterministic,
--      STRING-returning function (here Hive's own built-in UDFUUID, so no
--      external jar is needed) called in the outer SELECT list.
--   5. hive.optimize.dynamic.partition.hashjoin=true -- forces the join
--      into a reduce-side dynamic partitioned hash join instead of a
--      map-side broadcast join.
--   6. hive.vectorized.adaptor.usage.mode=chosen -- routes the temp
--      function (not in the natively-vectorized whitelist) through the
--      generic VectorUDFAdaptor, a row-by-row bridge between vectorized
--      ColumnVectors and boxed Writables. That bridge, combined with the
--      reduce-side hash join from (5), is where the type mismatch on
--      flag_cnt actually surfaces.
--
-- With CBO enabled (the default), this throws. With hive.cbo.enable=false,
-- it passes cleanly -- confirming the corruption originates in CBO's
-- handling of the temporary function within this join shape.

set hive.execution.engine=tez;
set hive.auto.convert.join=true;
-- Raise counter caps: this query's many nested join/group-by/select
-- operators exceed the default 120-counter limit, unrelated to the bug
-- itself ("Counters limit exceeded: Too many counters: 121 max=120").
set mapreduce.job.counters.max=10000;
set tez.counters.max=10000;
set tez.counters.max.groups=3000;

set hive.optimize.dynamic.partition.hashjoin=true;
set hive.vectorized.adaptor.usage.mode=chosen;


DROP TABLE IF EXISTS t_base;
CREATE TABLE t_base (
  entity_id  STRING,
  proc_type  STRING,
  event_dt   STRING,
  code_list  STRING,
  notif_cd   STRING,
  category_cd STRING,
  status_cd  STRING,
  amount_usd DOUBLE,
  flag_ind   STRING,
  flag_cnt   SMALLINT
) STORED AS ORC;

INSERT INTO TABLE t_base VALUES
  ('BIN001','EDIT','20260101','045','1','0','C',100.00,'N',CAST(0 AS SMALLINT)),
  ('BIN001','EDIT','20260101','045','1','1','C',200.00,'N',CAST(0 AS SMALLINT)),
  ('BIN001','EDIT','20260101','000','3','2','W',50.00,'N',CAST(0 AS SMALLINT)),
  ('BIN001','EDIT','20260101','000','1','0','A',75.00,'N',CAST(0 AS SMALLINT)),
  ('BIN001','EDIT','20260101','000','1','3','S',10.00,'N',CAST(0 AS SMALLINT)),
  ('BIN002','EDIT','20260101','000','1','1','C',300.00,'N',CAST(0 AS SMALLINT)),
  ('BIN002','EDIT','20260101','000','1','0','U',20.00,'N',CAST(0 AS SMALLINT)),
  ('BIN002','EDIT','20260101','000','1','4','T',5.00,'N',CAST(0 AS SMALLINT)),
  ('BIN003','EDIT','20260101','000','1','5','C',400.00,'Y',CAST(1 AS SMALLINT)),
  ('BIN003','EDIT','20260101','000','1','6','C',150.00,'Y',CAST(1 AS SMALLINT)),
  ('BIN003','EDIT','20260101','000','1','0','A',60.00,'N',CAST(0 AS SMALLINT)),
  ('BIN003','EDIT','20260101','000','1','1','W',30.00,'N',CAST(0 AS SMALLINT));

CREATE TEMPORARY FUNCTION test_uuid AS 'org.apache.hadoop.hive.ql.udf.UDFUUID';
SELECT coalesce(join_a.entity_id, join_b.entity_id) as entity_id,
       coalesce(join_a.warn_code_1, '000') as warn_code_1,
       coalesce(join_a.warn_1_total, 0) as warn_1_total,
       coalesce(join_a.notif_1_total, 0) as notif_1_total,
       coalesce(join_b.cat_0_tran_count, 0) as cat_0_tran_count,
       coalesce(if(join_b.cat_0_dollar_total > 9999999999.99, 9999999999.99, join_b.cat_0_dollar_total), 0) as cat_0_dollar_total,
       COALESCE(join_b.flag_cnt, 0) as flag_cnt,
       test_uuid() as row_id
FROM
    (select warn_agg.entity_id,
            warn_agg.warn_code_1,
            warn_agg.warn_1_total,
            notif_agg.notif_1_total
     from
         (Select d.entity_id,
                 COALESCE(d.code_list[0], '000') as warn_code_1,
                 cast(COALESCE(d.count_list[0], 0) as int) as warn_1_total
          from
              (SELECT c.entity_id, collect_list(c.code) as code_list, collect_list(c.count) as count_list
               FROM
                   (select b.entity_id, b.code, count(1) as count
                    from (
                        select entity_id, trim(code) as code
                        from (
                            select entity_id, split(CONCAT(SUBSTR(RPAD(TRIM(regexp_replace(code_list,'=','0')),6,'0'),0,3),'-',SUBSTR(RPAD(TRIM(regexp_replace(code_list,'=','0')),6,'0'),4,3)), '-') as code_array
                            from t_base where proc_type = 'EDIT' and event_dt in ('20260101')) a
                        lateral view explode(a.code_array) exploded as code) b
                    where b.code != '000'
                    group by b.entity_id, b.code) c
               GROUP BY c.entity_id order by c.entity_id) d) warn_agg
         join
         (Select e.entity_id,
                 cast(COALESCE(e.count_list[0], 0) as int) as notif_1_total
          from
              (SELECT d.entity_id, collect_list(d.notif_cd) as notif_cd_list, collect_list(d.count) as count_list
               FROM
                   (select b.entity_id, b.notif_cd, COALESCE(a.count, 0) as count
                    from
                        (select entity_id, trim(notif_cd) as notif_cd
                         from (
                             select distinct entity_id, split(CONCAT(SUBSTR('13',1,1),'-',SUBSTR('13',2,1)), '-') as notif_cd_array
                             from t_base where proc_type = 'EDIT' and event_dt in ('20260101')) a
                        lateral view explode(a.notif_cd_array) exploded as notif_cd) b
                    left outer join
                        (SELECT c.entity_id, c.notif_cd, count(1) as count
                         FROM (select entity_id, notif_cd from t_base
                               where proc_type = 'EDIT' and event_dt in ('20260101')) c
                         group by c.entity_id, c.notif_cd order by c.notif_cd) a
                    on a.entity_id = b.entity_id and a.notif_cd = b.notif_cd
                    order by b.entity_id, b.notif_cd) d
               GROUP BY d.entity_id) e) notif_agg
         on warn_agg.entity_id = notif_agg.entity_id) join_a
        full outer join
    (Select d.entity_id,
            cast(d.count_list[0] as int) as cat_0_tran_count,
            cast(d.amount_usd_list[0] as double) as cat_0_dollar_total,
            flag_agg.flag_cnt
     FROM
         (SELECT c.entity_id, collect_list(c.category_code) as category_code_list, collect_list(c.count) as count_list,
                 collect_list(cast(c.amount_usd as string)) as amount_usd_list
          FROM
              (select b.entity_id, b.category_code, COALESCE(a.count, 0) as count, COALESCE(a.amount_usd, 0.0) as amount_usd
               from
                   (select entity_id, trim(category_code) as category_code
                    from (
                        select distinct entity_id, split(CONCAT(SUBSTR('01',1,1),'-',SUBSTR('01',2,1)), '-') as category_code_array
                        from t_base where proc_type = 'EDIT' and event_dt in ('20260101')) a
                    lateral view explode(a.category_code_array) exploded as category_code) b
               left outer join
                   (select entity_id, category_cd, count(1) as count,
                           sum(case when status_cd in ('C','W','A','S','U','T') then amount_usd else 0 end) as amount_usd
                    from t_base where proc_type = 'EDIT' and event_dt in ('20260101')
                    group by entity_id, category_cd order by category_cd) a
               on a.entity_id = b.entity_id and a.category_cd = b.category_code
               order by b.entity_id, b.category_code) c
          GROUP BY c.entity_id) d
             left outer join
         (select entity_id, count(1) as flag_cnt from t_base
          where proc_type = 'EDIT' and event_dt in ('20260101') and flag_ind = 'Y'
          group by entity_id) flag_agg
         on d.entity_id = flag_agg.entity_id
    ) join_b
    on join_a.entity_id = join_b.entity_id;


DROP TEMPORARY FUNCTION test_uuid;
DROP TABLE t_base;
