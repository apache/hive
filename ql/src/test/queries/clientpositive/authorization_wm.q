--! qt:authorizer
set hive.cli.errors.ignore=true;


set user.name=ruser1;
explain authorization create resource plan rp;
create resource plan rp;

set user.name=hive_admin_user;
set role ADMIN;
explain authorization create resource plan rp;
create resource plan rp;

set user.name=ruser1;
explain authorization show resource plans;
explain authorization show resource plan rp;
explain authorization alter resource plan rp set query_parallelism = 5;
explain authorization drop resource plan rp;
explain authorization create pool rp.pool0 WITH ALLOC_FRACTION=1.0, QUERY_PARALLELISM=5, SCHEDULING_POLICY='default';
explain authorization create trigger rp.trigger0 WHEN BYTES_READ > '10GB' DO KILL;
explain authorization create user mapping 'joe' IN rp UNMANAGED;
show resource plans;
show resource plan rp;
alter resource plan rp set query_parallelism = 5;
drop resource plan rp;
create pool rp.pool0 WITH ALLOC_FRACTION=1.0, QUERY_PARALLELISM=5, SCHEDULING_POLICY='default';
create trigger rp.trigger0 WHEN BYTES_READ > '10GB' DO KILL;
create user mapping 'joe' IN rp UNMANAGED;

set user.name=hive_admin_user;
set role ADMIN;
explain authorization show resource plans;
explain authorization show resource plan rp;
explain authorization alter resource plan rp set query_parallelism = 5;
explain authorization drop resource plan rp;
explain authorization create pool rp.pool0 WITH ALLOC_FRACTION=1.0, QUERY_PARALLELISM=5, SCHEDULING_POLICY='default';
explain authorization create trigger rp.trigger0 WHEN BYTES_READ > '10GB' DO KILL;
explain authorization create user mapping 'joe' IN rp UNMANAGED;
show resource plans;
show resource plan rp;
alter resource plan rp set query_parallelism = 5;
drop resource plan rp;
create resource plan rp;
create pool rp.pool0 WITH ALLOC_FRACTION=1.0, QUERY_PARALLELISM=5, SCHEDULING_POLICY='default';
create trigger rp.trigger0 WHEN BYTES_READ > '10GB' DO KILL;
create user mapping 'joe' IN rp UNMANAGED;

set user.name=ruser1;
explain authorization alter pool rp.pool0 SET QUERY_PARALLELISM=4;
explain authorization alter trigger rp.trigger0 WHEN BYTES_READ > '15GB' DO KILL;
explain authorization alter user mapping 'joe' IN rp TO pool0;
explain authorization drop user mapping 'joe' IN rp;
explain authorization drop pool rp.pool0;
explain authorization drop trigger rp.trigger0;
alter pool rp.pool0 SET QUERY_PARALLELISM=4;
alter trigger rp.trigger0 WHEN BYTES_READ > '15GB' DO KILL;
alter user mapping 'joe' IN rp TO pool0;
drop user mapping 'joe' IN rp;
drop pool rp.pool0;
drop trigger rp.trigger0;

set user.name=hive_admin_user;
set role ADMIN;
explain authorization alter pool rp.pool0 SET QUERY_PARALLELISM=4;
explain authorization alter trigger rp.trigger0 WHEN BYTES_READ > '15GB' DO KILL;
explain authorization alter user mapping 'joe' IN rp TO pool0;
explain authorization drop user mapping 'joe' IN rp;
explain authorization drop pool rp.pool0;
explain authorization drop trigger rp.trigger0;
alter pool rp.pool0 SET QUERY_PARALLELISM=4;
alter trigger rp.trigger0 WHEN BYTES_READ > '15GB' DO KILL;
alter user mapping 'joe' IN rp TO pool0;
drop user mapping 'joe' IN rp;
drop pool rp.pool0;
drop trigger rp.trigger0;
