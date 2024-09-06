set hive.strict.timestamp.conversion=false;

create table test_num_ts_input(begin string, ts string);

insert into test_num_ts_input values('1653209895687','2022-05-22T15:58:15.931+07:00'),('1653209938316','2022-05-22T15:58:58.490+07:00'),('1653209962021','2022-05-22T15:59:22.191+07:00'),('1653210021993','2022-05-22T16:00:22.174+07:00');

set hive.vectorized.execution.enabled=false;

CREATE TABLE t_date_ctas AS
select
  CAST( CAST( `begin` AS BIGINT) / 1000  AS TIMESTAMP ) `begin`,
  CAST( DATE_FORMAT(CAST(regexp_replace(`ts`,'(\\d{4})-(\\d{2})-(\\d{2})T(\\d{2}):(\\d{2}):(\\d{2}).(\\d{3})\\+(\\d{2}):(\\d{2})','$1-$2-$3 $4:$5:$6.$7') AS TIMESTAMP ),'yyyyMMdd') as BIGINT ) `par_key`
FROM  test_num_ts_input;

