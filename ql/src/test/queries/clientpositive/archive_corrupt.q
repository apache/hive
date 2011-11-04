set hive.archive.enabled = true;
set hive.enforce.bucketing = true;

drop table tstsrcpart;

create table tstsrcpart like srcpart;

load data local inpath '../data/files/archive_corrupt.rc' overwrite into table tstsrcpart partition (ds='2008-04-08', hr='11');

insert overwrite table tstsrcpart partition (ds='2008-04-08', hr='12')
select key, value from srcpart where ds='2008-04-08' and hr='12';

insert overwrite table tstsrcpart partition (ds='2008-04-09', hr='11')
select key, value from srcpart where ds='2008-04-09' and hr='11';

insert overwrite table tstsrcpart partition (ds='2008-04-09', hr='12')
select key, value from srcpart where ds='2008-04-09' and hr='12';

-- EXCLUDE_HADOOP_MAJOR_VERSIONS(0.17, 0.18, 0.19)

describe extended tstsrcpart partition (ds='2008-04-08', hr='11');

alter table tstsrcpart archive partition (ds='2008-04-08', hr='11');

describe extended tstsrcpart partition (ds='2008-04-08', hr='11');

alter table tstsrcpart unarchive partition (ds='2008-04-08', hr='11');

