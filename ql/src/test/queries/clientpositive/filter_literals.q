--! qt:dataset:alltypesorc
set hive.cbo.fallback.strategy=NEVER;

-- test CBO explain plans
explain cbo
select * from alltypesorc where 'foo';

explain cbo
select * from alltypesorc where 'foo' and 'bar';

explain cbo
select * from alltypesorc where 'foo' or 'bar';

explain cbo
select * from alltypesorc where 'foo' and cint in (2, 3);

explain cbo 
select * from alltypesorc where not 'foo';

explain cbo
select * from alltypesorc where 1;

explain cbo
select * from alltypesorc where 1 and 2;

explain cbo
select * from alltypesorc where not 1;

-- test results from CBO and non-CBO paths
set hive.cbo.enable=true;
select count(*) from alltypesorc where 'foo';
set hive.cbo.enable=false;
select count(*) from alltypesorc where 'foo';

set hive.cbo.enable=true;
select count(*) from alltypesorc where 'true';
set hive.cbo.enable=false;
select count(*) from alltypesorc where 'true';

set hive.cbo.enable=true;
select count(*) from alltypesorc where 'false';
set hive.cbo.enable=false;
select count(*) from alltypesorc where 'false';

set hive.cbo.enable=true;
select count(*) from alltypesorc where true;
set hive.cbo.enable=false;
select count(*) from alltypesorc where true;

set hive.cbo.enable=true;
select count(*) from alltypesorc where false;
set hive.cbo.enable=false;
select count(*) from alltypesorc where false;

set hive.cbo.enable=true;
select count(*) from alltypesorc where 1;
set hive.cbo.enable=false;
select count(*) from alltypesorc where 1;

set hive.cbo.enable=true;
select count(*) from alltypesorc where 0;
set hive.cbo.enable=false;
select count(*) from alltypesorc where 0;

set hive.cbo.enable=true;
select count(*) from alltypesorc where -1;
set hive.cbo.enable=false;
select count(*) from alltypesorc where -1;

set hive.cbo.enable=true;
select count(*) from alltypesorc where null;
set hive.cbo.enable=false;
select count(*) from alltypesorc where null;
