set hive.query.results.cache.enabled=false;
set hive.explain.user=true;
set hive.semantic.analyzer.hook=org.apache.hadoop.hive.ql.hooks.AccurateEstimatesCheckerHook;

drop table if exists default.rx0;
drop table if exists default.sr0;

create table rx0 (r_reason_id string, r_reason_sk bigint);
create table sr0 (sr_reason_sk bigint);

insert into rx0 values ('AAAAAAAAAAAAAAAA',1),('AAAAAAAAGEAAAAAA',70),
('A_2',2),('A_3',3),('A_4',4),('A_5',5),('A_6',6),('A_7',7),('A_8',8),('A_9',9),('A_10',10),('A_11',11),('A_12',12),('A_13',13),('A_14',14),('A_15',15),('A_16',16),('A_17',17),('A_18',18),('A_19',19),('A_20',20),('A_21',21),('A_22',22),('A_23',23),('A_24',24),('A_25',25),('A_26',26),('A_27',27),('A_28',28),('A_29',29),('A_30',30),('A_31',31),('A_32',32),('A_33',33),('A_34',34),('A_35',35),('A_36',36),('A_37',37),('A_38',38),('A_39',39),('A_40',40),('A_41',41),('A_42',42),('A_43',43),('A_44',44),('A_45',45),('A_46',46),('A_47',47),('A_48',48),('A_49',49),('A_50',50),('A_51',51),('A_52',52),('A_53',53),('A_54',54),('A_55',55),('A_56',56),('A_57',57),('A_58',58),('A_59',59),('A_60',60),('A_61',61),('A_62',62),('A_63',63),('A_64',64),('A_65',65),('A_66',66),('A_67',67),('A_68',68),('A_69',69);

insert into sr0 values (NULL),(1),(2),(3),(4),(5),(6),(7),(8),(9),(10),
(11),(12),(13),(14),(15),(16),(17),(18),(19),(20),(21),(22),(23),(24),(25),
(26),(27),(28),(29),(30),(31),(32),(33),(34),(35),(36),(37),(38),(39),(40),
(41),(42),(43),(44),(45),(46),(47),(48),(49),(50),(51),(52),(53),(54),(55),
(56),(57),(58),(59),(60),(61),(62),(63),(64),(65),(66),(67),(68),(69),(70);

desc formatted sr0 sr_reason_sk;

insert into sr0 select a.* from sr0 a,sr0 b;
-- at this point: the sr0 will have 5112 rows

desc formatted sr0 sr_reason_sk;

analyze table sr0 compute statistics for columns;

desc formatted sr0 sr_reason_sk;

explain analyze select 1
from default.sr0  store_returns , default.rx0 reason
            where sr_reason_sk = r_reason_sk
              and r_reason_id = 'AAAAAAAAAAAAAAAA';

