set hive.test.mode=true;
set hive.test.mode.prefix=;

create table exim_department ( dep_id int) stored as textfile;

set hive.security.authorization.enabled=true;

!rm -rf ../build/ql/test/data/exports/exim_department;
export table exim_department to 'ql/test/data/exports/exim_department';

set hive.security.authorization.enabled=false;
drop table exim_department;

