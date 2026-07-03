SELECT 'Upgrading MetaStore schema from 4.2.0 to 4.3.0';

ALTER TABLE "HIVE_LOCKS" ADD COLUMN "HL_CATALOG" varchar(128) NOT NULL DEFAULT 'hive';
ALTER TABLE "MATERIALIZATION_REBUILD_LOCKS" ADD COLUMN "MRL_CAT_NAME" varchar(128) NOT NULL DEFAULT 'hive';

CREATE INDEX "MIN_HISTORY_WRITE_ID_IDX" ON "MIN_HISTORY_WRITE_ID" ("MH_DATABASE", "MH_TABLE", "MH_WRITEID");

-- HiveDAE: BINDING_ID must be nullable — the ON DELETE SET NULL FK (DETACH) and
-- bindingId-less ERASE/RELEASE audit rows write NULL here (package.jdo allows-null).
ALTER TABLE "ERASURE_RUN_AUDIT" ALTER COLUMN "BINDING_ID" DROP NOT NULL;

-- HiveDAE: the legacy single-policy column-policy surface is retired; bindings
-- live in ERASURE_POLICY_BINDINGS (+ members). Drop the orphaned legacy table.
DROP TABLE IF EXISTS "COL_POLICIES";

-- These lines need to be last. Insert any changes above.
UPDATE "VERSION" SET "SCHEMA_VERSION"='4.3.0', "VERSION_COMMENT"='Hive release version 4.3.0' where "VER_ID"=1;
SELECT 'Finished upgrading MetaStore schema from 4.2.0 to 4.3.0';
