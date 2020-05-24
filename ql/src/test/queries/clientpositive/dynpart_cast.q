set hive.stats.autogather=true;

drop table dynpart_cast;
create table dynpart_cast (i int) PARTITIONED BY (`static_part` int, `dyn_part` int);

EXPLAIN
INSERT INTO TABLE dynpart_cast PARTITION (static_part=03, dyn_part)
SELECT 1,
'002';

-- stats task will fail here if dynamic partition not cast to integer and creates "dyn_part=002"
INSERT INTO TABLE dynpart_cast PARTITION (static_part=03, dyn_part)
SELECT 1,
'002';
