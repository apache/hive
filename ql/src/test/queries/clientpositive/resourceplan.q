-- Continue on errors, we do check some error conditions below.
set hive.cli.errors.ignore=true;

-- Prevent NPE in calcite.
set hive.cbo.enable=false;

-- Force DN to create db_privs tables.
show grant user hive_test_user;

-- Initialize the hive schema.
source ../../metastore/scripts/upgrade/hive/hive-schema-3.0.0.hive.sql;

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
CREATE RESOURCE PLAN plan_2 WITH QUERY_PARALLELISM=4;
SHOW RESOURCE PLANS;
SHOW RESOURCE PLAN plan_2;
SELECT * FROM SYS.WM_RESOURCEPLANS;

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

-- Use reserved keyword table as name.
CREATE RESOURCE PLAN `table`;
ALTER RESOURCE PLAN `table` SET QUERY_PARALLELISM = 1;
SELECT * FROM SYS.WM_RESOURCEPLANS;

--
-- Create trigger commands.
--

CREATE RESOURCE PLAN plan_1;

CREATE TRIGGER plan_1.trigger_1 WHEN BYTES_READ > 10k AND BYTES_READ <= 1M OR ELAPSED_TIME > 30 SECOND AND ELAPSED_TIME < 1 MINUTE DO KILL;
SELECT * FROM SYS.WM_TRIGGERS;

-- Duplicate should fail.
CREATE TRIGGER plan_1.trigger_1 WHEN BYTES_READ = 10G DO KILL;

CREATE TRIGGER plan_1.trigger_2 WHEN BYTES_READ > 100 DO MOVE TO slow_pool;
SELECT * FROM SYS.WM_TRIGGERS;

ALTER TRIGGER plan_1.trigger_1 WHEN BYTES_READ = 1000 DO KILL;
SELECT * FROM SYS.WM_TRIGGERS;

DROP TRIGGER plan_1.trigger_1;
SELECT * FROM SYS.WM_TRIGGERS;

-- No edit on active resource plan.
CREATE TRIGGER plan_2.trigger_1 WHEN BYTES_READ = 0m DO MOVE TO null_pool;

-- Add trigger with reserved keywords.
CREATE TRIGGER `table`.`table` WHEN BYTES_WRITTEN > 100K DO MOVE TO `table`;
CREATE TRIGGER `table`.`trigger` WHEN BYTES_WRITTEN > 100K DO MOVE TO `default`;
CREATE TRIGGER `table`.`database` WHEN BYTES_WRITTEN > 1M DO MOVE TO `default`;
CREATE TRIGGER `table`.`trigger1` WHEN ELAPSED_TIME > 10 DO KILL;
CREATE TRIGGER `table`.`trigger2` WHEN BYTES_READ > 100 DO KILL;
SELECT * FROM SYS.WM_TRIGGERS;
DROP TRIGGER `table`.`database`;
SELECT * FROM SYS.WM_TRIGGERS;

-- Cannot drop/change trigger from enabled plan.
ALTER RESOURCE PLAN plan_1 ENABLE;
SELECT * FROM SYS.WM_RESOURCEPLANS;
DROP TRIGGER plan_1.trigger_2;
ALTER TRIGGER plan_1.trigger_2 WHEN BYTES_READ = 1000g DO KILL;

-- Cannot drop/change trigger from active plan.
ALTER RESOURCE PLAN plan_1 ACTIVATE;
SELECT * FROM SYS.WM_RESOURCEPLANS;
DROP TRIGGER plan_1.trigger_2;
ALTER TRIGGER plan_1.trigger_2 WHEN BYTES_READ = 1000K DO KILL;

-- Once disabled we should be able to change it.
ALTER RESOURCE PLAN plan_2 DISABLE;
CREATE TRIGGER plan_2.trigger_1 WHEN BYTES_READ = 0 DO MOVE TO null_pool;
SELECT * FROM SYS.WM_TRIGGERS;


--
-- Create pool command.
--

-- Cannot create pool in active plans.
CREATE POOL plan_1.default WITH
   ALLOC_FRACTION=1.0, QUERY_PARALLELISM=5, SCHEDULING_POLICY='default';

CREATE POOL plan_2.default WITH
   ALLOC_FRACTION=1.0, QUERY_PARALLELISM=5, SCHEDULING_POLICY='default';
SELECT * FROM SYS.WM_POOLS;

CREATE POOL plan_2.default.c1 WITH
    ALLOC_FRACTION=0.3, QUERY_PARALLELISM=3, SCHEDULING_POLICY='priority';

CREATE POOL plan_2.default.c2 WITH
    QUERY_PARALLELISM=2, SCHEDULING_POLICY='fair', ALLOC_FRACTION=0.2;

-- Cannot activate c1 + c2 = 0.5
ALTER RESOURCE PLAN plan_2 VALIDATE;
ALTER RESOURCE PLAN plan_2 ENABLE ACTIVATE;

ALTER POOL plan_2.default.c2 SET ALLOC_FRACTION = 0.7, QUERY_PARALLELISM = 1;

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
    QUERY_PARALLELISM=2, SCHEDULING_POLICY='fcfs', ALLOC_FRACTION=0.8;

-- Create nested pools.
CREATE POOL `table`.`table` WITH
  SCHEDULING_POLICY='random', ALLOC_FRACTION=0.5, QUERY_PARALLELISM=1;

CREATE POOL `table`.`table`.pool1 WITH
  SCHEDULING_POLICY='priority', QUERY_PARALLELISM=3, ALLOC_FRACTION=0.9;
CREATE POOL `table`.`table`.pool1.child1 WITH
  SCHEDULING_POLICY='random', QUERY_PARALLELISM=1, ALLOC_FRACTION=0.3;
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
ALTER POOL `table`.`table`.pool.child2 ADD TRIGGER `trigger1`;
ALTER POOL `table`.`table`.pool.child2 ADD TRIGGER `trigger2`;
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
CREATE GROUP MAPPING 'group2' IN plan_2 TO def.c2 WITH ORDER 1;
SELECT * FROM SYS.WM_MAPPINGS;

-- Drop pool failed, pool in use.
DROP POOL plan_2.def.c1;

DROP USER MAPPING "user2" in plan_2;
DROP GROUP MAPPING "group2" in plan_2;
SELECT * FROM SYS.WM_MAPPINGS;

CREATE RESOURCE PLAN plan_4;

ALTER RESOURCE PLAN plan_4 ENABLE ACTIVATE;

DROP RESOURCE PLAN plan_2;

SELECT * FROM SYS.WM_RESOURCEPLANS;
SELECT * FROM SYS.WM_POOLS;
SELECT * FROM SYS.WM_TRIGGERS;
SELECT * FROM SYS.WM_POOLS_TO_TRIGGERS;
SELECT * FROM SYS.WM_MAPPINGS;
