-- HIVE-30037: CBO declines scripts; ORDER BY positions count the TRANSFORM output columns.
-- Runs on TestMiniLlapCliDriver: hive.llap.execution.mode=only on the local driver rejects script operators.

create table pacd_tr (d int, s string);
insert into pacd_tr values (1, 'b'), (3, 'a'), (2, 'c');

select transform(d, s) using 'cat' as (x, y) from pacd_tr order by 2 desc;
