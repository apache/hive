-- Continue on errors, we do check some error conditions below.
set hive.cli.errors.ignore=true;
set hive.test.authz.sstd.hs2.mode=true;

-- Prevent NPE in calcite.
set hive.cbo.enable=false;

-- Force DN to create db_privs tables.
show grant user hive_test_user;

-- Initialize the hive schema.
source ../../metastore/scripts/upgrade/hive/hive-schema-3.1.0.hive.sql;

-- SORT_QUERY_RESULTS

--
-- Actual tests.
--

-- Empty resource plans.
SHOW RESOURCE PLANS;
SELECT * FROM SYS.WM_RESOURCEPLANS;

-- Create and show plan_1.
CREATE RESOURCE PLAN plan_1;
SHOW RESOURCE PLANS;
SHOW RESOURCE PLAN plan_1;
SELECT * FROM SYS.WM_RESOURCEPLANS;

-- Create and show plan_2.
CREATE RESOURCE PLAN plan_2 WITH QUERY_PARALLELISM=5;
ALTER RESOURCE PLAN plan_2 SET QUERY_PARALLELISM=10;
SHOW RESOURCE PLANS;
SHOW RESOURCE PLAN plan_2;
SELECT * FROM SYS.WM_RESOURCEPLANS;

-- Create plan with existing name, should fail
CREATE RESOURCE PLAN plan_2;
-- Create plan with existing name with IF NOT EXISTS
CREATE RESOURCE PLAN IF NOT EXISTS plan_2;

-- Should fail cannot set pool in create.
CREATE RESOURCE PLAN plan_3 WITH QUERY_PARALLELISM=5, DEFAULT POOL = `all`;

--
-- Rename resource plans.
--

-- Fail, duplicate name.
ALTER RESOURCE PLAN plan_1 RENAME TO plan_2;
SELECT * FROM SYS.WM_RESOURCEPLANS;

-- Success.
ALTER RESOURCE PLAN plan_1 RENAME TO plan_3;
SELECT * FROM SYS.WM_RESOURCEPLANS;

-- Change query parallelism, success.
ALTER RESOURCE PLAN plan_3 SET QUERY_PARALLELISM = 4;
SELECT * FROM SYS.WM_RESOURCEPLANS;

-- Change query parallelism, success.
ALTER RESOURCE PLAN plan_3 UNSET QUERY_PARALLELISM;
SELECT * FROM SYS.WM_RESOURCEPLANS;


-- Will fail for now; there are no pools.
ALTER RESOURCE PLAN plan_3 SET QUERY_PARALLELISM = 30, DEFAULT POOL = default1;
SELECT * FROM SYS.WM_RESOURCEPLANS;

-- Shouldn't be able to rename or modify an enabled plan.
ALTER RESOURCE PLAN plan_3 ENABLE;
ALTER RESOURCE PLAN plan_3 RENAME TO plan_4;
ALTER RESOURCE PLAN plan_3 SET QUERY_PARALLELISM = 30;
ALTER RESOURCE PLAN plan_3 DISABLE;
SELECT * FROM SYS.WM_RESOURCEPLANS;

--
-- Activate, enable, disable.
--

-- DISABLED -> ACTIVE fail.
ALTER RESOURCE PLAN plan_3 ACTIVATE;
SELECT * FROM SYS.WM_RESOURCEPLANS;

-- DISABLED -> DISABLED success.
ALTER RESOURCE PLAN plan_3 DISABLE;
SELECT * FROM SYS.WM_RESOURCEPLANS;

-- DISABLED -> ENABLED success.
ALTER RESOURCE PLAN plan_3 ENABLE;
SELECT * FROM SYS.WM_RESOURCEPLANS;

-- ENABLED -> ACTIVE success.
ALTER RESOURCE PLAN plan_3 ACTIVATE;
SELECT * FROM SYS.WM_RESOURCEPLANS;

-- ACTIVE -> ACTIVE success.
ALTER RESOURCE PLAN plan_3 ACTIVATE;
SELECT * FROM SYS.WM_RESOURCEPLANS;

-- ACTIVE -> ENABLED fail.
ALTER RESOURCE PLAN plan_3 ENABLE;
SELECT * FROM SYS.WM_RESOURCEPLANS;

-- ACTIVE -> DISABLED fail.
ALTER RESOURCE PLAN plan_3 DISABLE;
SELECT * FROM SYS.WM_RESOURCEPLANS;

-- DISABLE WM - ok.
DISABLE WORKLOAD MANAGEMENT;
SELECT * FROM SYS.WM_RESOURCEPLANS;

ALTER RESOURCE PLAN plan_3 DISABLE;

-- Enable + activate ok.
ALTER RESOURCE PLAN plan_3 ENABLE ACTIVATE;
SELECT * FROM SYS.WM_RESOURCEPLANS;

-- DISABLED -> ENABLED success.
ALTER RESOURCE PLAN plan_2 ENABLE;
SELECT * FROM SYS.WM_RESOURCEPLANS;

-- plan_2: ENABLED -> ACTIVE and plan_3: ACTIVE -> ENABLED, success.
ALTER RESOURCE PLAN plan_2 ACTIVATE;
SELECT * FROM SYS.WM_RESOURCEPLANS;

-- ENABLED -> ENABLED success.
ALTER RESOURCE PLAN plan_3 ENABLE;
SELECT * FROM SYS.WM_RESOURCEPLANS;

-- ENABLED -> DISABLED success.
ALTER RESOURCE PLAN plan_3 DISABLE;
SELECT * FROM SYS.WM_RESOURCEPLANS;

--
-- Drop resource plan.
--

-- Fail, active plan.
DROP RESOURCE PLAN plan_2;

-- Success.
DROP RESOURCE PLAN plan_3;
SELECT * FROM SYS.WM_RESOURCEPLANS;

-- Drop non existing resource plan, should fail
DROP RESOURCE PLAN plan_99999;
-- Drop non existing resource plan with IF EXISTS
DROP RESOURCE PLAN IF EXISTS plan_99999;

-- Use reserved keyword table as name.
CREATE RESOURCE PLAN `table`;
ALTER RESOURCE PLAN `table` SET QUERY_PARALLELISM = 1;
SELECT * FROM SYS.WM_RESOURCEPLANS;

--
-- Create trigger commands.
--

-- Test that WM literals do not cause conflicts.
create table wm_test(key string);
select key as 30min from wm_test;
select "10kb" as str from wm_test;
drop table wm_test;

CREATE RESOURCE PLAN plan_1;

CREATE TRIGGER plan_1.trigger_1 WHEN BYTES_READ > '10kb' DO KILL;
SELECT * FROM SYS.WM_TRIGGERS;

-- Duplicate should fail.
CREATE TRIGGER plan_1.trigger_1 WHEN ELAPSED_TIME > 300 DO KILL;

-- Invalid triggers should fail.
CREATE TRIGGER plan_1.trigger_2 WHEN ELAPSED_TIME > '30sec' AND BYTES_READ > 10 DO MOVE TO slow_pool;
CREATE TRIGGER plan_1.trigger_2 WHEN ELAPSED_TIME > '30second' OR BYTES_READ > 10 DO MOVE TO slow_pool;
CREATE TRIGGER plan_1.trigger_2 WHEN ELAPSED_TIME >= '30seconds' DO MOVE TO slow_pool;
CREATE TRIGGER plan_1.trigger_2 WHEN ELAPSED_TIME < '30hour' DO MOVE TO slow_pool;
CREATE TRIGGER plan_1.trigger_2 WHEN ELAPSED_TIME <= '30min' DO MOVE TO slow_pool;
CREATE TRIGGER plan_1.trigger_2 WHEN ELAPSED_TIME = '0day' DO MOVE TO slow_pool;

CREATE TRIGGER plan_1.trigger_2 WHEN ELAPSED_TIME > '30hour' DO MOVE TO slow_pool;
SELECT * FROM SYS.WM_TRIGGERS;

ALTER TRIGGER plan_1.trigger_1 WHEN BYTES_READ > '1min' DO KILL;
SELECT * FROM SYS.WM_TRIGGERS;

DROP TRIGGER plan_1.trigger_1;
SELECT * FROM SYS.WM_TRIGGERS;

-- No edit on active resource plan.
CREATE TRIGGER plan_2.trigger_1 WHEN BYTES_READ > '100mb' DO MOVE TO null_pool;

-- Add trigger with reserved keywords.
CREATE TRIGGER `table`.`table` WHEN BYTES_WRITTEN > '100KB' DO MOVE TO `table`;
CREATE TRIGGER `table`.`trigger` WHEN BYTES_WRITTEN > '100MB' DO MOVE TO `default`;
CREATE TRIGGER `table`.`database` WHEN BYTES_WRITTEN > "1GB" DO MOVE TO `default`;
CREATE TRIGGER `table`.`trigger1` WHEN ELAPSED_TIME > 10 DO KILL;
CREATE TRIGGER `table`.`trigger2` WHEN ELAPSED_TIME > '1hour' DO KILL;
SELECT * FROM SYS.WM_TRIGGERS;
DROP TRIGGER `table`.`database`;
SELECT * FROM SYS.WM_TRIGGERS;

-- Cannot drop/change trigger from enabled plan.
ALTER RESOURCE PLAN plan_1 ENABLE;
SELECT * FROM SYS.WM_RESOURCEPLANS;
DROP TRIGGER plan_1.trigger_2;
ALTER TRIGGER plan_1.trigger_2 WHEN BYTES_READ > "1000gb" DO KILL;

-- Cannot drop/change trigger from active plan.
ALTER RESOURCE PLAN plan_1 ACTIVATE;
SELECT * FROM SYS.WM_RESOURCEPLANS;
DROP TRIGGER plan_1.trigger_2;
ALTER TRIGGER plan_1.trigger_2 WHEN BYTES_READ > "1000KB" DO KILL;

-- Once disabled we should be able to change it.
ALTER RESOURCE PLAN plan_2 DISABLE;
CREATE TRIGGER plan_2.trigger_1 WHEN BYTES_READ > 0 DO MOVE TO null_pool;
SELECT * FROM SYS.WM_TRIGGERS;


--
-- Create pool command.
--

-- Cannot create pool in active plans.
CREATE POOL plan_1.default WITH
   ALLOC_FRACTION=1.0, QUERY_PARALLELISM=5, SCHEDULING_POLICY='default';

CREATE POOL plan_2.default WITH QUERY_PARALLELISM=5, SCHEDULING_POLICY='default';
CREATE POOL plan_2.default WITH ALLOC_FRACTION=1.0;
CREATE POOL plan_2.default WITH ALLOC_FRACTION=1.0, QUERY_PARALLELISM=5;
SELECT * FROM SYS.WM_POOLS;

CREATE POOL plan_2.default.c1 WITH
    ALLOC_FRACTION=0.3, QUERY_PARALLELISM=3, SCHEDULING_POLICY='invalid';

CREATE POOL plan_2.default.c1 WITH
    ALLOC_FRACTION=0.3, QUERY_PARALLELISM=3, SCHEDULING_POLICY='fair';

CREATE POOL plan_2.default.c2 WITH
    QUERY_PARALLELISM=2, SCHEDULING_POLICY='fair', ALLOC_FRACTION=0.7;

-- Cannot activate c1 + c2 = 1.0
ALTER RESOURCE PLAN plan_2 VALIDATE;
ALTER RESOURCE PLAN plan_2 ENABLE ACTIVATE;

ALTER POOL plan_2.default.c2 SET ALLOC_FRACTION = 0.5, QUERY_PARALLELISM = 1;
ALTER POOL plan_2.default.c2 SET SCHEDULING_POLICY='fair';
SELECT * FROM SYS.WM_POOLS;
ALTER POOL plan_2.default.c2 UNSET SCHEDULING_POLICY;
SELECT * FROM SYS.WM_POOLS;

-- Now we can activate.
ALTER RESOURCE PLAN plan_2 VALIDATE;
ALTER RESOURCE PLAN plan_2 ENABLE ACTIVATE;
ALTER RESOURCE PLAN plan_1 ACTIVATE;
ALTER RESOURCE PLAN plan_2 DISABLE;

ALTER POOL plan_2.default SET path = def;
SELECT * FROM SYS.WM_POOLS;

DROP POOL plan_2.default;
SELECT * FROM SYS.WM_POOLS;

-- Create failed no parent pool found.
CREATE POOL plan_2.child1.child2 WITH
    QUERY_PARALLELISM=2, SCHEDULING_POLICY='fifo', ALLOC_FRACTION=0.8;

-- Create nested pools.
CREATE POOL `table`.`table` WITH
  SCHEDULING_POLICY='fifo', ALLOC_FRACTION=0.5, QUERY_PARALLELISM=1;

CREATE POOL `table`.`table`.pool1 WITH
  SCHEDULING_POLICY='fair', QUERY_PARALLELISM=3, ALLOC_FRACTION=0.9;
CREATE POOL `table`.`table`.pool1.child1 WITH
  SCHEDULING_POLICY='fair', QUERY_PARALLELISM=1, ALLOC_FRACTION=0.3;
CREATE POOL `table`.`table`.pool1.child2 WITH
  SCHEDULING_POLICY='fair', QUERY_PARALLELISM=3, ALLOC_FRACTION=0.7;
ALTER POOL `table`.`table` SET ALLOC_FRACTION=0.0;
SELECT * FROM SYS.WM_POOLS;

-- Rename with child pools and parent pool.
ALTER POOL `table`.`table`.pool1 SET PATH = `table`.pool;
SELECT * FROM SYS.WM_POOLS;

-- Fails has child pools.
DROP POOL `table`.`table`;
SELECT * FROM SYS.WM_POOLS;

-- Fails default is default pool :-).
DROP POOL `table`.default;
SELECT * FROM SYS.WM_POOLS;
SELECT * FROM SYS.WM_RESOURCEPLANS;

-- Changed default pool, now it should work.
ALTER RESOURCE PLAN `table` SET DEFAULT POOL = `table`.pool;
DROP POOL `table`.default;
SELECT * FROM SYS.WM_POOLS;

-- Change query parallelism, success.
ALTER RESOURCE PLAN `table` UNSET DEFAULT POOL;
SELECT * FROM SYS.WM_RESOURCEPLANS;

--
-- Pool to trigger mappings.
--

-- Success.
ALTER POOL plan_2.def.c1 ADD TRIGGER trigger_1;
ALTER POOL plan_2.def.c2 ADD TRIGGER trigger_1;

-- With keywords, hopefully nobody does this.
ALTER POOL `table`.`table` ADD TRIGGER `table`;

-- Test m:n mappings.
ALTER POOL `table`.`table`.pool.child1 ADD TRIGGER `table`;
ALTER POOL `table`.`table`.pool.child1 ADD TRIGGER `trigger1`;
ALTER TRIGGER `table`.`trigger1` ADD TO POOL `table`.pool.child2;
ALTER POOL `table`.`table`.pool.child2 ADD TRIGGER `trigger2`;
ALTER TRIGGER `table`.`trigger1` ADD TO UNMANAGED; 
SELECT * FROM SYS.WM_POOLS_TO_TRIGGERS;

SHOW RESOURCE PLAN `table`;

ALTER TRIGGER `table`.`trigger1` DROP FROM POOL `table`.pool.child2;
ALTER TRIGGER `table`.`trigger1` DROP FROM UNMANAGED; 
SELECT * FROM SYS.WM_POOLS_TO_TRIGGERS;

-- Failures.


-- pool does not exist.
ALTER POOL plan_2.default ADD TRIGGER trigger_1;

-- Trigger does not exist.
ALTER POOL plan_2.def ADD TRIGGER trigger_2;

SELECT * FROM SYS.WM_POOLS_TO_TRIGGERS;

-- Drop success.
ALTER POOL plan_2.def.c1 DROP TRIGGER trigger_1;

-- Drop fail, does not exist.
ALTER POOL plan_2.def.c1 DROP TRIGGER trigger_2;

-- Drops related mappings too.
DROP POOL `table`.`table`.pool.child1;
DROP POOL `table`.`table`.pool.child2;

SELECT * FROM SYS.WM_POOLS_TO_TRIGGERS;


--
-- User and group mappings.
--

CREATE USER MAPPING "user1" IN plan_2 TO def;
CREATE USER MAPPING 'user2' IN plan_2 TO def WITH ORDER 1;
CREATE GROUP MAPPING "group1" IN plan_2 TO def.c1;
CREATE APPLICATION MAPPING "app1" IN plan_2 TO def.c1;
CREATE GROUP MAPPING 'group2' IN plan_2 TO def.c2 WITH ORDER 1;
CREATE GROUP MAPPING 'group3' IN plan_2 UNMANAGED WITH ORDER 1;
ALTER USER MAPPING "user1" IN plan_2 UNMANAGED;

SHOW RESOURCE PLAN plan_2;

SELECT * FROM SYS.WM_MAPPINGS;

-- Drop pool failed, pool in use.
DROP POOL plan_2.def.c1;

DROP USER MAPPING "user2" in plan_2;
DROP GROUP MAPPING "group2" in plan_2;
DROP GROUP MAPPING "group3" in plan_2;
DROP APPLICATION MAPPING "app1" in plan_2;
SELECT * FROM SYS.WM_MAPPINGS;

CREATE RESOURCE PLAN plan_4;

ALTER RESOURCE PLAN plan_4 ENABLE ACTIVATE;

SHOW RESOURCE PLAN plan_2;

-- This should remove all pools, triggers & mappings. 
DROP RESOURCE PLAN plan_2;

-- This should create plan_2 with default pool and null query parallelism.
CREATE RESOURCE PLAN plan_2;

SELECT * FROM SYS.WM_RESOURCEPLANS;
SELECT * FROM SYS.WM_POOLS;
SELECT * FROM SYS.WM_TRIGGERS;
SELECT * FROM SYS.WM_POOLS_TO_TRIGGERS;
SELECT * FROM SYS.WM_MAPPINGS;

-- Create like another plan; modify, replace. Create all manner of things to make sure LIKE works.
CREATE RESOURCE PLAN plan_4a LIKE plan_4;
CREATE POOL plan_4a.pool1 WITH SCHEDULING_POLICY='fair', QUERY_PARALLELISM=2, ALLOC_FRACTION=0.0;
CREATE USER MAPPING "user1" IN plan_4a TO pool1;
CREATE TRIGGER plan_4a.trigger_1 WHEN BYTES_READ > '10GB' DO KILL;
CREATE TRIGGER plan_4a.trigger_2 WHEN BYTES_READ > '11GB' DO KILL;
ALTER POOL plan_4a.pool1 ADD TRIGGER trigger_2;

CREATE RESOURCE PLAN plan_4b LIKE plan_4a;
CREATE POOL plan_4b.pool2 WITH SCHEDULING_POLICY='fair', QUERY_PARALLELISM=3, ALLOC_FRACTION=0.0;
SELECT * FROM SYS.WM_RESOURCEPLANS;
SELECT * FROM SYS.WM_POOLS;
SELECT * FROM SYS.WM_TRIGGERS;
SELECT * FROM SYS.WM_POOLS_TO_TRIGGERS;
SELECT * FROM SYS.WM_MAPPINGS;

REPLACE RESOURCE PLAN plan_4a WITH plan_4b;
SELECT * FROM SYS.WM_RESOURCEPLANS;
SELECT * FROM SYS.WM_POOLS;
SHOW RESOURCE PLAN plan_4a_old_0;
REPLACE ACTIVE RESOURCE PLAN WITH plan_4a;
SELECT * FROM SYS.WM_RESOURCEPLANS;
CREATE RESOURCE PLAN plan_4a LIKE plan_4;
CREATE POOL plan_4a.pool3 WITH SCHEDULING_POLICY='fair', QUERY_PARALLELISM=3, ALLOC_FRACTION=0.0;
ALTER RESOURCE PLAN plan_4a ENABLE ACTIVATE WITH REPLACE;
SELECT * FROM SYS.WM_RESOURCEPLANS;
SELECT * FROM SYS.WM_POOLS;


