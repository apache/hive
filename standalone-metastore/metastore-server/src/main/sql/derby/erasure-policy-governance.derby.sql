-- Erasure policy governance schema additions.
--
-- These nine tables back the entities introduced in the Thrift IDL for the
-- versioned-policy + multi-policy-binding + audit model. In standard Hive
-- Metastore deployments DataNucleus creates the tables automatically from
-- the package.jdo mapping on first access; this script exists as canonical
-- documentation and for environments that prefer pre-provisioning.
--
-- All foreign-key columns are intentionally permissive about cross-table
-- references (no ON DELETE CASCADE) so that audit history survives policy
-- deletion and re-creation.

-- ---------------------------------------------------------------------------
-- 1. ERASURE_POLICY_VERSIONS
-- ---------------------------------------------------------------------------
CREATE TABLE "APP"."ERASURE_POLICY_VERSIONS" (
    "VERSION_ID"           BIGINT       NOT NULL,
    "POLICY_ID"            BIGINT       NOT NULL,    -- FK to existing ERASURE_POLICIES
    "VERSION_LABEL"        VARCHAR(64)  NOT NULL,
    "STATUS"               VARCHAR(16)  NOT NULL,    -- DRAFT|VALIDATED|ACTIVE|SUPERSEDED|INACTIVE
    "IDENTITY_FIELD_NAME"  VARCHAR(128) NOT NULL,
    "IDENTITY_FIELD_TYPE"  VARCHAR(8)   NOT NULL,    -- INT|LONG|STRING
    "SCHEMA_TYPE"          VARCHAR(8)   NOT NULL,
    "SOURCE_PATH"          VARCHAR(1024),
    "SOURCE_CHECKSUM"      CHAR(64),
    "VALIDATED_BY"         VARCHAR(256),
    "VALIDATED_TS"         BIGINT,
    "ACTIVATED_BY"         VARCHAR(256),
    "ACTIVATED_TS"         BIGINT,
    "DEACTIVATED_BY"       VARCHAR(256),
    "DEACTIVATED_TS"       BIGINT,
    PRIMARY KEY ("VERSION_ID"),
    CONSTRAINT "EPV_UNIQ_LABEL" UNIQUE ("POLICY_ID", "VERSION_LABEL")
);
CREATE INDEX "EPV_BY_POLICY"  ON "APP"."ERASURE_POLICY_VERSIONS" ("POLICY_ID", "STATUS");

-- ---------------------------------------------------------------------------
-- 2. ERASURE_POLICY_STATEMENTS  (one per FOR SCHEMA block)
-- ---------------------------------------------------------------------------
CREATE TABLE "APP"."ERASURE_POLICY_STATEMENTS" (
    "STATEMENT_ID"  BIGINT       NOT NULL,
    "VERSION_ID"    BIGINT       NOT NULL,
    "SCHEMA_VALUE"  VARCHAR(256) NOT NULL,
    "ORDINAL"       INTEGER      NOT NULL,
    PRIMARY KEY ("STATEMENT_ID")
);
CREATE INDEX "EPS_BY_VERSION" ON "APP"."ERASURE_POLICY_STATEMENTS" ("VERSION_ID", "ORDINAL");

-- ---------------------------------------------------------------------------
-- 3. ERASURE_POLICY_RULES  (one per ERASE/REPLACE/HASH/... rule)
-- ---------------------------------------------------------------------------
CREATE TABLE "APP"."ERASURE_POLICY_RULES" (
    "RULE_ID"        BIGINT        NOT NULL,
    "STATEMENT_ID"   BIGINT        NOT NULL,
    "FIELD_PATH"     VARCHAR(512)  NOT NULL,
    "ACTION"         VARCHAR(16)   NOT NULL,    -- ERASE|REPLACE|HASH|TOKENIZE|ENCRYPT|GENERALIZE
    "LITERAL_VALUE"  VARCHAR(1024),
    "LITERAL_TYPE"   VARCHAR(8),                -- INT|LONG|STRING
    "PARAMS"         CLOB,                       -- JSON blob for action-specific params
    "ORDINAL"        INTEGER       NOT NULL,
    PRIMARY KEY ("RULE_ID")
);
CREATE INDEX "EPR_BY_STMT" ON "APP"."ERASURE_POLICY_RULES" ("STATEMENT_ID", "ORDINAL");

-- ---------------------------------------------------------------------------
-- 4. ERASURE_POLICY_BINDINGS  (multi-policy replacement for COL_POLICIES)
-- ---------------------------------------------------------------------------
CREATE TABLE "APP"."ERASURE_POLICY_BINDINGS" (
    "BINDING_ID"      BIGINT       NOT NULL,
    "TBL_ID"          BIGINT       NOT NULL,    -- FK to existing TBLS
    "COLUMN_NAME"     VARCHAR(128) NOT NULL,
    "SCHEMA_FIELD"    VARCHAR(128) NOT NULL,
    "ROW_LOCATOR"     VARCHAR(128) NOT NULL,
    "COLUMN_FORMAT"   VARCHAR(16)  NOT NULL,    -- JSON|MSGPACK|XML|PROTOBUF|AVRO
    "RESOLUTION_MODE" VARCHAR(16)  NOT NULL,    -- EXPLICIT|STRICTEST
    "CREATED_BY"      VARCHAR(256) NOT NULL,
    "CREATED_TS"      BIGINT       NOT NULL,
    PRIMARY KEY ("BINDING_ID"),
    CONSTRAINT "EPB_UNIQ_COL" UNIQUE ("TBL_ID", "COLUMN_NAME")
);
CREATE INDEX "EPB_BY_TABLE" ON "APP"."ERASURE_POLICY_BINDINGS" ("TBL_ID");

-- ---------------------------------------------------------------------------
-- 5. ERASURE_POLICY_BINDING_MEMBERS  (M:N: bindings × policies)
-- ---------------------------------------------------------------------------
CREATE TABLE "APP"."ERASURE_POLICY_BINDING_MEMBERS" (
    "BINDING_ID"  BIGINT  NOT NULL,
    "POLICY_ID"   BIGINT  NOT NULL,
    "ORDINAL"     INTEGER NOT NULL,
    PRIMARY KEY ("BINDING_ID", "POLICY_ID")
);
CREATE INDEX "EPBM_BY_POLICY" ON "APP"."ERASURE_POLICY_BINDING_MEMBERS" ("POLICY_ID");

-- ---------------------------------------------------------------------------
-- 6. ERASURE_POLICY_BINDING_RESOLVED  (materialised §5.6 output)
-- ---------------------------------------------------------------------------
CREATE TABLE "APP"."ERASURE_POLICY_BINDING_RESOLVED" (
    "RESOLVED_ID"             BIGINT        NOT NULL,
    "BINDING_ID"              BIGINT        NOT NULL,
    "SCHEMA_VALUE"            VARCHAR(256)  NOT NULL,
    "FIELD_PATH"              VARCHAR(512)  NOT NULL,
    "ACTION"                  VARCHAR(16)   NOT NULL,
    "LITERAL_VALUE"           VARCHAR(1024),
    "LITERAL_TYPE"            VARCHAR(8),
    "PARAMS"                  CLOB,
    "CONTRIBUTING_POLICIES"   CLOB          NOT NULL,  -- JSON array of POLICY_IDs
    "RESOLUTION_NOTE"         VARCHAR(256),
    PRIMARY KEY ("RESOLVED_ID")
);
CREATE INDEX "EPBR_BY_BINDING" ON "APP"."ERASURE_POLICY_BINDING_RESOLVED" ("BINDING_ID", "SCHEMA_VALUE");

-- ---------------------------------------------------------------------------
-- 7. ERASURE_POLICY_LIFECYCLE_EVENTS  (append-only audit log)
-- ---------------------------------------------------------------------------
CREATE TABLE "APP"."ERASURE_POLICY_LIFECYCLE_EVENTS" (
    "EVENT_ID"       BIGINT       NOT NULL,
    "VERSION_ID"     BIGINT       NOT NULL,
    "EVENT_TYPE"     VARCHAR(24)  NOT NULL,    -- VALIDATED|ACTIVATED|DEACTIVATED|SUPERSEDED|BOUND|UNBOUND|ATTACH_REJECTED
    "PRINCIPAL"      VARCHAR(256) NOT NULL,
    "EVENT_TS"       BIGINT       NOT NULL,
    "NOTE"           VARCHAR(2048),
    "CONFLICT_CLASS" VARCHAR(16),                -- populated only for ATTACH_REJECTED
    "BINDING_ID"     BIGINT,                     -- populated for binding-scoped events
    PRIMARY KEY ("EVENT_ID")
);
CREATE INDEX "EPLE_BY_VERSION" ON "APP"."ERASURE_POLICY_LIFECYCLE_EVENTS" ("VERSION_ID", "EVENT_TS");
CREATE INDEX "EPLE_BY_BINDING" ON "APP"."ERASURE_POLICY_LIFECYCLE_EVENTS" ("BINDING_ID", "EVENT_TS");
CREATE INDEX "EPLE_BY_TYPE"    ON "APP"."ERASURE_POLICY_LIFECYCLE_EVENTS" ("EVENT_TYPE", "EVENT_TS");

-- ---------------------------------------------------------------------------
-- 8. ERASURE_RUN_AUDIT  (per-execution audit of ERASE FROM TABLE)
-- ---------------------------------------------------------------------------
CREATE TABLE "APP"."ERASURE_RUN_AUDIT" (
    "RUN_ID"                       BIGINT       NOT NULL,
    "TBL_ID"                       BIGINT       NOT NULL,
    "COLUMN_NAME"                  VARCHAR(128) NOT NULL,
    "BINDING_ID"                   BIGINT       NOT NULL,
    "PRINCIPAL"                    VARCHAR(256) NOT NULL,
    "STARTED_TS"                   BIGINT       NOT NULL,
    "COMPLETED_TS"                 BIGINT,
    "IDENTITY_VALUES"              CLOB,                    -- hashed by default (§5.8 privacy paradox)
    "POLICY_VERSIONS"              CLOB,                    -- JSON array of VERSION_IDs
    "RESOLVED_RULES_SNAPSHOT_ID"   BIGINT,
    "FILES_REWRITTEN"              INTEGER,
    "BYTES_BEFORE"                 BIGINT,
    "BYTES_AFTER"                  BIGINT,
    "STATUS"                       VARCHAR(16)  NOT NULL,    -- SUCCEEDED|FAILED|INTERRUPTED
    PRIMARY KEY ("RUN_ID")
);
CREATE INDEX "ERA_BY_TABLE"   ON "APP"."ERASURE_RUN_AUDIT" ("TBL_ID", "STARTED_TS");
CREATE INDEX "ERA_BY_BINDING" ON "APP"."ERASURE_RUN_AUDIT" ("BINDING_ID", "STARTED_TS");
CREATE INDEX "ERA_BY_USER"    ON "APP"."ERASURE_RUN_AUDIT" ("PRINCIPAL", "STARTED_TS");
CREATE INDEX "ERA_BY_STATUS"  ON "APP"."ERASURE_RUN_AUDIT" ("STATUS", "STARTED_TS");

-- ---------------------------------------------------------------------------
-- 9. POLICY_PRIVS  (privilege grants on policy objects)
-- ---------------------------------------------------------------------------
CREATE TABLE "APP"."POLICY_PRIVS" (
    "POLICY_PRIV_ID"  BIGINT       NOT NULL,
    "POLICY_ID"       BIGINT       NOT NULL,    -- 0 for wildcard *
    "PRINCIPAL_NAME"  VARCHAR(256) NOT NULL,
    "PRINCIPAL_TYPE"  VARCHAR(16)  NOT NULL,    -- USER|ROLE
    "PRIVILEGE"       VARCHAR(32)  NOT NULL,    -- POLICY_VALIDATE|POLICY_ACTIVATE|POLICY_MANAGE
    "CREATE_TIME"     BIGINT       NOT NULL,
    "GRANTOR"         VARCHAR(256),
    "GRANTOR_TYPE"    VARCHAR(16),
    "GRANT_OPTION"    SMALLINT     NOT NULL DEFAULT 0,
    PRIMARY KEY ("POLICY_PRIV_ID"),
    CONSTRAINT "PP_UNIQ_GRANT" UNIQUE ("POLICY_ID", "PRINCIPAL_NAME", "PRINCIPAL_TYPE", "PRIVILEGE")
);
CREATE INDEX "PP_BY_PRINCIPAL" ON "APP"."POLICY_PRIVS" ("PRINCIPAL_NAME", "PRINCIPAL_TYPE");
CREATE INDEX "PP_BY_POLICY"    ON "APP"."POLICY_PRIVS" ("POLICY_ID");
