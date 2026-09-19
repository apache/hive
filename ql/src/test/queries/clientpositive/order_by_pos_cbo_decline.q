-- HIVE-30037: CBO declines TABLESAMPLE; ORDER BY 1 DESC must return 3, 2, 1.
-- MiniLlapLocal: mvn test -pl itests/qtest -Dtest=TestMiniLlapLocalCliDriver -Dqfile=order_by_pos_cbo_decline.q
-- Overwrite golden: add -Dtest.output.overwrite=true

set hive.fetch.task.conversion=none;
set hive.cbo.enable=true;

create table hive30037_ob_t (d int);

insert into hive30037_ob_t values (1), (2), (3);

select d from hive30037_ob_t tablesample (5 rows) s order by 1 desc;
