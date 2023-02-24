--
-- PostgreSQL database dump
--

SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = off;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET escape_string_warning = off;

SET search_path = public, pg_catalog;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: BUCKETING_COLS; Type: TABLE; Schema: public; Owner: hiveuser; Tablespace:
--

CREATE TABLE "BUCKETING_COLS" (
    "SD_ID" bigint NOT NULL,
    "BUCKET_COL_NAME" character varying(256) DEFAULT NULL::character varying,
    "INTEGER_IDX" bigint NOT NULL
);


--
-- Name: CDS; Type: TABLE; Schema: public; Owner: hiveuser; Tablespace:
--

CREATE TABLE "CDS" (
    "CD_ID" bigint NOT NULL
);


--
-- Name: COLUMNS_V2; Type: TABLE; Schema: public; Owner: hiveuser; Tablespace:
--

CREATE TABLE "COLUMNS_V2" (
    "CD_ID" bigint NOT NULL,
    "COMMENT" character varying(4000),
    "COLUMN_NAME" character varying(767) NOT NULL,
    "TYPE_NAME" text,
    "INTEGER_IDX" integer NOT NULL
);


--
-- Name: DATABASE_PARAMS; Type: TABLE; Schema: public; Owner: hiveuser; Tablespace:
--

CREATE TABLE "DATABASE_PARAMS" (
    "DB_ID" bigint NOT NULL,
    "PARAM_KEY" character varying(180) NOT NULL,
    "PARAM_VALUE" character varying(4000) DEFAULT NULL::character varying
);


CREATE TABLE "CTLGS" (
    "CTLG_ID" BIGINT PRIMARY KEY,
    "NAME" VARCHAR(256) UNIQUE,
    "DESC" VARCHAR(4000),
    "LOCATION_URI" VARCHAR(4000) NOT NULL,
    "CREATE_TIME" bigint
);

-- Insert a default value.  The location is TBD.  Hive will fix this when it starts
INSERT INTO "CTLGS" VALUES (1, 'hive', 'Default catalog for Hive', 'TBD', NULL);

--
-- Name: DBS; Type: TABLE; Schema: public; Owner: hiveuser; Tablespace:
--

CREATE TABLE "DBS" (
    "DB_ID" bigint NOT NULL,
    "DESC" character varying(4000) DEFAULT NULL::character varying,
    "DB_LOCATION_URI" character varying(4000) NOT NULL,
    "NAME" character varying(128) DEFAULT NULL::character varying,
    "OWNER_NAME" character varying(128) DEFAULT NULL::character varying,
    "OWNER_TYPE" character varying(10) DEFAULT NULL::character varying,
    "CTLG_NAME" varchar(256) DEFAULT 'hive' NOT NULL,
    "CREATE_TIME" bigint,
    "DB_MANAGED_LOCATION_URI" character varying(4000),
    "TYPE" character varying(32) DEFAULT 'NATIVE' NOT NULL,
    "DATACONNECTOR_NAME" character varying(128),
    "REMOTE_DBNAME" character varying(128)
);


--
-- Name: DB_PRIVS; Type: TABLE; Schema: public; Owner: hiveuser; Tablespace:
--

CREATE TABLE "DB_PRIVS" (
    "DB_GRANT_ID" bigint NOT NULL,
    "CREATE_TIME" bigint NOT NULL,
    "DB_ID" bigint,
    "GRANT_OPTION" smallint NOT NULL,
    "GRANTOR" character varying(128) DEFAULT NULL::character varying,
    "GRANTOR_TYPE" character varying(128) DEFAULT NULL::character varying,
    "PRINCIPAL_NAME" character varying(128) DEFAULT NULL::character varying,
    "PRINCIPAL_TYPE" character varying(128) DEFAULT NULL::character varying,
    "DB_PRIV" character varying(128) DEFAULT NULL::character varying,
    "AUTHORIZER" character varying(128) DEFAULT NULL::character varying
);


--
-- Name: DC_PRIVS; Type: TABLE; Schema: public; Owner: hiveuser; Tablespace:
--

CREATE TABLE "DC_PRIVS" (
                            "DC_GRANT_ID" bigint NOT NULL,
                            "CREATE_TIME" bigint NOT NULL,
                            "NAME" character varying(128),
                            "GRANT_OPTION" smallint NOT NULL,
                            "GRANTOR" character varying(128) DEFAULT NULL::character varying,
                            "GRANTOR_TYPE" character varying(128) DEFAULT NULL::character varying,
                            "PRINCIPAL_NAME" character varying(128) DEFAULT NULL::character varying,
                            "PRINCIPAL_TYPE" character varying(128) DEFAULT NULL::character varying,
                            "DC_PRIV" character varying(128) DEFAULT NULL::character varying,
                            "AUTHORIZER" character varying(128) DEFAULT NULL::character varying
);


--
-- Name: GLOBAL_PRIVS; Type: TABLE; Schema: public; Owner: hiveuser; Tablespace:
--

CREATE TABLE "GLOBAL_PRIVS" (
    "USER_GRANT_ID" bigint NOT NULL,
    "CREATE_TIME" bigint NOT NULL,
    "GRANT_OPTION" smallint NOT NULL,
    "GRANTOR" character varying(128) DEFAULT NULL::character varying,
    "GRANTOR_TYPE" character varying(128) DEFAULT NULL::character varying,
    "PRINCIPAL_NAME" character varying(128) DEFAULT NULL::character varying,
    "PRINCIPAL_TYPE" character varying(128) DEFAULT NULL::character varying,
    "USER_PRIV" character varying(128) DEFAULT NULL::character varying,
    "AUTHORIZER" character varying(128) DEFAULT NULL::character varying
);


--
-- Name: IDXS; Type: TABLE; Schema: public; Owner: hiveuser; Tablespace:
--

CREATE TABLE "IDXS" (
    "INDEX_ID" bigint NOT NULL,
    "CREATE_TIME" bigint NOT NULL,
    "DEFERRED_REBUILD" boolean NOT NULL,
    "INDEX_HANDLER_CLASS" character varying(4000) DEFAULT NULL::character varying,
    "INDEX_NAME" character varying(128) DEFAULT NULL::character varying,
    "INDEX_TBL_ID" bigint,
    "LAST_ACCESS_TIME" bigint NOT NULL,
    "ORIG_TBL_ID" bigint,
    "SD_ID" bigint
);


--
-- Name: INDEX_PARAMS; Type: TABLE; Schema: public; Owner: hiveuser; Tablespace:
--

CREATE TABLE "INDEX_PARAMS" (
    "INDEX_ID" bigint NOT NULL,
    "PARAM_KEY" character varying(256) NOT NULL,
    "PARAM_VALUE" character varying(4000) DEFAULT NULL::character varying
);


--
-- Name: NUCLEUS_TABLES; Type: TABLE; Schema: public; Owner: hiveuser; Tablespace:
--

CREATE TABLE "NUCLEUS_TABLES" (
    "CLASS_NAME" character varying(128) NOT NULL,
    "TABLE_NAME" character varying(128) NOT NULL,
    "TYPE" character varying(4) NOT NULL,
    "OWNER" character varying(2) NOT NULL,
    "VERSION" character varying(20) NOT NULL,
    "INTERFACE_NAME" character varying(255) DEFAULT NULL::character varying
);


--
-- Name: PARTITIONS; Type: TABLE; Schema: public; Owner: hiveuser; Tablespace:
--

CREATE TABLE "PARTITIONS" (
    "PART_ID" bigint NOT NULL,
    "CREATE_TIME" bigint NOT NULL,
    "LAST_ACCESS_TIME" bigint NOT NULL,
    "PART_NAME" character varying(767) DEFAULT NULL::character varying,
    "SD_ID" bigint,
    "TBL_ID" bigint,
    "WRITE_ID" bigint DEFAULT 0
);


--
-- Name: PARTITION_EVENTS; Type: TABLE; Schema: public; Owner: hiveuser; Tablespace:
--

CREATE TABLE "PARTITION_EVENTS" (
    "PART_NAME_ID" bigint NOT NULL,
    "CAT_NAME" character varying(256),
    "DB_NAME" character varying(128),
    "EVENT_TIME" bigint NOT NULL,
    "EVENT_TYPE" integer NOT NULL,
    "PARTITION_NAME" character varying(767),
    "TBL_NAME" character varying(256)
);


--
-- Name: PARTITION_KEYS; Type: TABLE; Schema: public; Owner: hiveuser; Tablespace:
--

CREATE TABLE "PARTITION_KEYS" (
    "TBL_ID" bigint NOT NULL,
    "PKEY_COMMENT" character varying(4000) DEFAULT NULL::character varying,
    "PKEY_NAME" character varying(128) NOT NULL,
    "PKEY_TYPE" character varying(767) NOT NULL,
    "INTEGER_IDX" bigint NOT NULL
);


--
-- Name: PARTITION_KEY_VALS; Type: TABLE; Schema: public; Owner: hiveuser; Tablespace:
--

CREATE TABLE "PARTITION_KEY_VALS" (
    "PART_ID" bigint NOT NULL,
    "PART_KEY_VAL" character varying(256) DEFAULT NULL::character varying,
    "INTEGER_IDX" bigint NOT NULL
);


--
-- Name: PARTITION_PARAMS; Type: TABLE; Schema: public; Owner: hiveuser; Tablespace:
--

CREATE TABLE "PARTITION_PARAMS" (
    "PART_ID" bigint NOT NULL,
    "PARAM_KEY" character varying(256) NOT NULL,
    "PARAM_VALUE" text DEFAULT NULL
);


--
-- Name: PART_COL_PRIVS; Type: TABLE; Schema: public; Owner: hiveuser; Tablespace:
--

CREATE TABLE "PART_COL_PRIVS" (
    "PART_COLUMN_GRANT_ID" bigint NOT NULL,
    "COLUMN_NAME" character varying(767) DEFAULT NULL::character varying,
    "CREATE_TIME" bigint NOT NULL,
    "GRANT_OPTION" smallint NOT NULL,
    "GRANTOR" character varying(128) DEFAULT NULL::character varying,
    "GRANTOR_TYPE" character varying(128) DEFAULT NULL::character varying,
    "PART_ID" bigint,
    "PRINCIPAL_NAME" character varying(128) DEFAULT NULL::character varying,
    "PRINCIPAL_TYPE" character varying(128) DEFAULT NULL::character varying,
    "PART_COL_PRIV" character varying(128) DEFAULT NULL::character varying,
    "AUTHORIZER" character varying(128) DEFAULT NULL::character varying
);


--
-- Name: PART_PRIVS; Type: TABLE; Schema: public; Owner: hiveuser; Tablespace:
--

CREATE TABLE "PART_PRIVS" (
    "PART_GRANT_ID" bigint NOT NULL,
    "CREATE_TIME" bigint NOT NULL,
    "GRANT_OPTION" smallint NOT NULL,
    "GRANTOR" character varying(128) DEFAULT NULL::character varying,
    "GRANTOR_TYPE" character varying(128) DEFAULT NULL::character varying,
    "PART_ID" bigint,
    "PRINCIPAL_NAME" character varying(128) DEFAULT NULL::character varying,
    "PRINCIPAL_TYPE" character varying(128) DEFAULT NULL::character varying,
    "PART_PRIV" character varying(128) DEFAULT NULL::character varying,
    "AUTHORIZER" character varying(128) DEFAULT NULL::character varying
);


--
-- Name: ROLES; Type: TABLE; Schema: public; Owner: hiveuser; Tablespace:
--

CREATE TABLE "ROLES" (
    "ROLE_ID" bigint NOT NULL,
    "CREATE_TIME" bigint NOT NULL,
    "OWNER_NAME" character varying(128) DEFAULT NULL::character varying,
    "ROLE_NAME" character varying(128) DEFAULT NULL::character varying
);


--
-- Name: ROLE_MAP; Type: TABLE; Schema: public; Owner: hiveuser; Tablespace:
--

CREATE TABLE "ROLE_MAP" (
    "ROLE_GRANT_ID" bigint NOT NULL,
    "ADD_TIME" bigint NOT NULL,
    "GRANT_OPTION" smallint NOT NULL,
    "GRANTOR" character varying(128) DEFAULT NULL::character varying,
    "GRANTOR_TYPE" character varying(128) DEFAULT NULL::character varying,
    "PRINCIPAL_NAME" character varying(128) DEFAULT NULL::character varying,
    "PRINCIPAL_TYPE" character varying(128) DEFAULT NULL::character varying,
    "ROLE_ID" bigint
);


--
-- Name: SDS; Type: TABLE; Schema: public; Owner: hiveuser; Tablespace:
--

CREATE TABLE "SDS" (
    "SD_ID" bigint NOT NULL,
    "INPUT_FORMAT" character varying(4000) DEFAULT NULL::character varying,
    "IS_COMPRESSED" boolean NOT NULL,
    "LOCATION" character varying(4000) DEFAULT NULL::character varying,
    "NUM_BUCKETS" bigint NOT NULL,
    "OUTPUT_FORMAT" character varying(4000) DEFAULT NULL::character varying,
    "SERDE_ID" bigint,
    "CD_ID" bigint,
    "IS_STOREDASSUBDIRECTORIES" boolean NOT NULL
);


--
-- Name: SD_PARAMS; Type: TABLE; Schema: public; Owner: hiveuser; Tablespace:
--

CREATE TABLE "SD_PARAMS" (
    "SD_ID" bigint NOT NULL,
    "PARAM_KEY" character varying(256) NOT NULL,
    "PARAM_VALUE" text DEFAULT NULL
);


--
-- Name: SEQUENCE_TABLE; Type: TABLE; Schema: public; Owner: hiveuser; Tablespace:
--

CREATE TABLE "SEQUENCE_TABLE" (
    "SEQUENCE_NAME" character varying(255) NOT NULL,
    "NEXT_VAL" bigint NOT NULL
);

INSERT INTO "SEQUENCE_TABLE" ("SEQUENCE_NAME", "NEXT_VAL") VALUES ('org.apache.hadoop.hive.metastore.model.MNotificationLog', 1);

--
-- Name: SERDES; Type: TABLE; Schema: public; Owner: hiveuser; Tablespace:
--

CREATE TABLE "SERDES" (
    "SERDE_ID" bigint NOT NULL,
    "NAME" character varying(128) DEFAULT NULL::character varying,
    "SLIB" character varying(4000) DEFAULT NULL::character varying,
    "DESCRIPTION" varchar(4000),
    "SERIALIZER_CLASS" varchar(4000),
    "DESERIALIZER_CLASS" varchar(4000),
    "SERDE_TYPE" integer
);


--
-- Name: SERDE_PARAMS; Type: TABLE; Schema: public; Owner: hiveuser; Tablespace:
--

CREATE TABLE "SERDE_PARAMS" (
    "SERDE_ID" bigint NOT NULL,
    "PARAM_KEY" character varying(256) NOT NULL,
    "PARAM_VALUE" text DEFAULT NULL
);


--
-- Name: SORT_COLS; Type: TABLE; Schema: public; Owner: hiveuser; Tablespace:
--

CREATE TABLE "SORT_COLS" (
    "SD_ID" bigint NOT NULL,
    "COLUMN_NAME" character varying(767) DEFAULT NULL::character varying,
    "ORDER" bigint NOT NULL,
    "INTEGER_IDX" bigint NOT NULL
);


--
-- Name: TABLE_PARAMS; Type: TABLE; Schema: public; Owner: hiveuser; Tablespace:
--

CREATE TABLE "TABLE_PARAMS" (
    "TBL_ID" bigint NOT NULL,
    "PARAM_KEY" character varying(256) NOT NULL,
    "PARAM_VALUE" text DEFAULT NULL
);


--
-- Name: TBLS; Type: TABLE; Schema: public; Owner: hiveuser; Tablespace:
--

CREATE TABLE "TBLS" (
    "TBL_ID" bigint NOT NULL,
    "CREATE_TIME" bigint NOT NULL,
    "DB_ID" bigint,
    "LAST_ACCESS_TIME" bigint NOT NULL,
    "OWNER" character varying(767) DEFAULT NULL::character varying,
    "OWNER_TYPE" character varying(10) DEFAULT NULL::character varying,
    "RETENTION" bigint NOT NULL,
    "SD_ID" bigint,
    "TBL_NAME" character varying(256) DEFAULT NULL::character varying,
    "TBL_TYPE" character varying(128) DEFAULT NULL::character varying,
    "VIEW_EXPANDED_TEXT" text,
    "VIEW_ORIGINAL_TEXT" text,
    "IS_REWRITE_ENABLED" boolean NOT NULL DEFAULT false,
    "WRITE_ID" bigint DEFAULT 0
);

--
-- Name: MV_CREATION_METADATA; Type: TABLE; Schema: public; Owner: hiveuser; Tablespace:
--

CREATE TABLE "MV_CREATION_METADATA" (
    "MV_CREATION_METADATA_ID" bigint NOT NULL,
    "CAT_NAME" character varying(256) NOT NULL,
    "DB_NAME" character varying(128) NOT NULL,
    "TBL_NAME" character varying(256) NOT NULL,
    "TXN_LIST" text,
    "MATERIALIZATION_TIME" bigint NOT NULL
);

--
-- Name: MV_TABLES_USED; Type: TABLE; Schema: public; Owner: hiveuser; Tablespace:
--

CREATE TABLE "MV_TABLES_USED" (
    "MV_CREATION_METADATA_ID" bigint NOT NULL,
    "TBL_ID" bigint NOT NULL,
    "INSERTED_COUNT" bigint NOT NULL DEFAULT 0,
    "UPDATED_COUNT" bigint NOT NULL DEFAULT 0,
    "DELETED_COUNT" bigint NOT NULL DEFAULT 0,
    CONSTRAINT "MV_TABLES_USED_PK" PRIMARY KEY ("TBL_ID", "MV_CREATION_METADATA_ID")
);

--
-- Name: TBL_COL_PRIVS; Type: TABLE; Schema: public; Owner: hiveuser; Tablespace:
--

CREATE TABLE "TBL_COL_PRIVS" (
    "TBL_COLUMN_GRANT_ID" bigint NOT NULL,
    "COLUMN_NAME" character varying(767) DEFAULT NULL::character varying,
    "CREATE_TIME" bigint NOT NULL,
    "GRANT_OPTION" smallint NOT NULL,
    "GRANTOR" character varying(128) DEFAULT NULL::character varying,
    "GRANTOR_TYPE" character varying(128) DEFAULT NULL::character varying,
    "PRINCIPAL_NAME" character varying(128) DEFAULT NULL::character varying,
    "PRINCIPAL_TYPE" character varying(128) DEFAULT NULL::character varying,
    "TBL_COL_PRIV" character varying(128) DEFAULT NULL::character varying,
    "TBL_ID" bigint,
    "AUTHORIZER" character varying(128) DEFAULT NULL::character varying
);


--
-- Name: TBL_PRIVS; Type: TABLE; Schema: public; Owner: hiveuser; Tablespace:
--

CREATE TABLE "TBL_PRIVS" (
    "TBL_GRANT_ID" bigint NOT NULL,
    "CREATE_TIME" bigint NOT NULL,
    "GRANT_OPTION" smallint NOT NULL,
    "GRANTOR" character varying(128) DEFAULT NULL::character varying,
    "GRANTOR_TYPE" character varying(128) DEFAULT NULL::character varying,
    "PRINCIPAL_NAME" character varying(128) DEFAULT NULL::character varying,
    "PRINCIPAL_TYPE" character varying(128) DEFAULT NULL::character varying,
    "TBL_PRIV" character varying(128) DEFAULT NULL::character varying,
    "TBL_ID" bigint,
    "AUTHORIZER" character varying(128) DEFAULT NULL::character varying
);


--
-- Name: TYPES; Type: TABLE; Schema: public; Owner: hiveuser; Tablespace:
--

CREATE TABLE "TYPES" (
    "TYPES_ID" bigint NOT NULL,
    "TYPE_NAME" character varying(128) DEFAULT NULL::character varying,
    "TYPE1" character varying(767) DEFAULT NULL::character varying,
    "TYPE2" character varying(767) DEFAULT NULL::character varying
);


--
-- Name: TYPE_FIELDS; Type: TABLE; Schema: public; Owner: hiveuser; Tablespace:
--

CREATE TABLE "TYPE_FIELDS" (
    "TYPE_NAME" bigint NOT NULL,
    "COMMENT" character varying(256) DEFAULT NULL::character varying,
    "FIELD_NAME" character varying(128) NOT NULL,
    "FIELD_TYPE" character varying(767) NOT NULL,
    "INTEGER_IDX" bigint NOT NULL
);

CREATE TABLE "SKEWED_STRING_LIST" (
    "STRING_LIST_ID" bigint NOT NULL
);

CREATE TABLE "SKEWED_STRING_LIST_VALUES" (
    "STRING_LIST_ID" bigint NOT NULL,
    "STRING_LIST_VALUE" character varying(256) DEFAULT NULL::character varying,
    "INTEGER_IDX" bigint NOT NULL
);

CREATE TABLE "SKEWED_COL_NAMES" (
    "SD_ID" bigint NOT NULL,
    "SKEWED_COL_NAME" character varying(256) DEFAULT NULL::character varying,
    "INTEGER_IDX" bigint NOT NULL
);

CREATE TABLE "SKEWED_COL_VALUE_LOC_MAP" (
    "SD_ID" bigint NOT NULL,
    "STRING_LIST_ID_KID" bigint NOT NULL,
    "LOCATION" character varying(4000) DEFAULT NULL::character varying
);

CREATE TABLE "SKEWED_VALUES" (
    "SD_ID_OID" bigint NOT NULL,
    "STRING_LIST_ID_EID" bigint NOT NULL,
    "INTEGER_IDX" bigint NOT NULL
);


--
-- Name: TAB_COL_STATS Type: TABLE; Schema: public; Owner: hiveuser; Tablespace:
--

CREATE TABLE  "MASTER_KEYS"
(
    "KEY_ID" SERIAL,
    "MASTER_KEY" varchar(767) NULL,
    PRIMARY KEY ("KEY_ID")
);

CREATE TABLE  "DELEGATION_TOKENS"
(
    "TOKEN_IDENT" varchar(767) NOT NULL,
    "TOKEN" varchar(767) NULL,
    PRIMARY KEY ("TOKEN_IDENT")
);

CREATE TABLE "TAB_COL_STATS" (
 "CS_ID" bigint NOT NULL,
 "CAT_NAME" character varying(256) DEFAULT NULL::character varying,
 "DB_NAME" character varying(128) DEFAULT NULL::character varying,
 "TABLE_NAME" character varying(256) DEFAULT NULL::character varying,
 "COLUMN_NAME" character varying(767) DEFAULT NULL::character varying,
 "COLUMN_TYPE" character varying(128) DEFAULT NULL::character varying,
 "TBL_ID" bigint NOT NULL,
 "LONG_LOW_VALUE" bigint,
 "LONG_HIGH_VALUE" bigint,
 "DOUBLE_LOW_VALUE" double precision,
 "DOUBLE_HIGH_VALUE" double precision,
 "BIG_DECIMAL_LOW_VALUE" character varying(4000) DEFAULT NULL::character varying,
 "BIG_DECIMAL_HIGH_VALUE" character varying(4000) DEFAULT NULL::character varying,
 "NUM_NULLS" bigint NOT NULL,
 "NUM_DISTINCTS" bigint,
 "BIT_VECTOR" bytea,
 "AVG_COL_LEN" double precision,
 "MAX_COL_LEN" bigint,
 "NUM_TRUES" bigint,
 "NUM_FALSES" bigint,
 "LAST_ANALYZED" bigint NOT NULL,
 "ENGINE" character varying(128) NOT NULL
);

--
-- Table structure for VERSION
--
CREATE TABLE "VERSION" (
  "VER_ID" bigint,
  "SCHEMA_VERSION" character varying(127) NOT NULL,
  "VERSION_COMMENT" character varying(255) NOT NULL
);

--
-- Name: PART_COL_STATS Type: TABLE; Schema: public; Owner: hiveuser; Tablespace:
--

CREATE TABLE "PART_COL_STATS" (
 "CS_ID" bigint NOT NULL,
 "CAT_NAME" character varying(256) DEFAULT NULL::character varying,
 "DB_NAME" character varying(128) DEFAULT NULL::character varying,
 "TABLE_NAME" character varying(256) DEFAULT NULL::character varying,
 "PARTITION_NAME" character varying(767) DEFAULT NULL::character varying,
 "COLUMN_NAME" character varying(767) DEFAULT NULL::character varying,
 "COLUMN_TYPE" character varying(128) DEFAULT NULL::character varying,
 "PART_ID" bigint NOT NULL,
 "LONG_LOW_VALUE" bigint,
 "LONG_HIGH_VALUE" bigint,
 "DOUBLE_LOW_VALUE" double precision,
 "DOUBLE_HIGH_VALUE" double precision,
 "BIG_DECIMAL_LOW_VALUE" character varying(4000) DEFAULT NULL::character varying,
 "BIG_DECIMAL_HIGH_VALUE" character varying(4000) DEFAULT NULL::character varying,
 "NUM_NULLS" bigint NOT NULL,
 "NUM_DISTINCTS" bigint,
 "BIT_VECTOR" bytea,
 "AVG_COL_LEN" double precision,
 "MAX_COL_LEN" bigint,
 "NUM_TRUES" bigint,
 "NUM_FALSES" bigint,
 "LAST_ANALYZED" bigint NOT NULL,
 "ENGINE" character varying(128) NOT NULL
);

--
-- Table structure for FUNCS
--
CREATE TABLE "FUNCS" (
  "FUNC_ID" BIGINT NOT NULL,
  "CLASS_NAME" VARCHAR(4000),
  "CREATE_TIME" INTEGER NOT NULL,
  "DB_ID" BIGINT,
  "FUNC_NAME" VARCHAR(128),
  "FUNC_TYPE" INTEGER NOT NULL,
  "OWNER_NAME" VARCHAR(128),
  "OWNER_TYPE" VARCHAR(10),
  PRIMARY KEY ("FUNC_ID")
);

--
-- Table structure for FUNC_RU
--
CREATE TABLE "FUNC_RU" (
  "FUNC_ID" BIGINT NOT NULL,
  "RESOURCE_TYPE" INTEGER NOT NULL,
  "RESOURCE_URI" VARCHAR(4000),
  "INTEGER_IDX" INTEGER NOT NULL,
  PRIMARY KEY ("FUNC_ID", "INTEGER_IDX")
);

CREATE TABLE "NOTIFICATION_LOG"
(
    "NL_ID" BIGINT NOT NULL,
    "EVENT_ID" BIGINT NOT NULL,
    "EVENT_TIME" INTEGER NOT NULL,
    "EVENT_TYPE" VARCHAR(32) NOT NULL,
    "CAT_NAME" VARCHAR(256),
    "DB_NAME" VARCHAR(128),
    "TBL_NAME" VARCHAR(256),
    "MESSAGE" text,
    "MESSAGE_FORMAT" VARCHAR(16),
    PRIMARY KEY ("NL_ID")
);

CREATE UNIQUE INDEX "NOTIFICATION_LOG_EVENT_ID" ON "NOTIFICATION_LOG" USING btree ("EVENT_ID");

CREATE TABLE "NOTIFICATION_SEQUENCE"
(
    "NNI_ID" BIGINT NOT NULL,
    "NEXT_EVENT_ID" BIGINT NOT NULL,
    PRIMARY KEY ("NNI_ID")
);

INSERT INTO "NOTIFICATION_SEQUENCE" ("NNI_ID", "NEXT_EVENT_ID") SELECT 1,1 WHERE NOT EXISTS ( SELECT "NEXT_EVENT_ID" FROM "NOTIFICATION_SEQUENCE");

CREATE TABLE "KEY_CONSTRAINTS"
(
  "CHILD_CD_ID" BIGINT,
  "CHILD_INTEGER_IDX" BIGINT,
  "CHILD_TBL_ID" BIGINT,
  "PARENT_CD_ID" BIGINT,
  "PARENT_INTEGER_IDX" BIGINT NOT NULL,
  "PARENT_TBL_ID" BIGINT NOT NULL,
  "POSITION" BIGINT NOT NULL,
  "CONSTRAINT_NAME" VARCHAR(400) NOT NULL,
  "CONSTRAINT_TYPE" SMALLINT NOT NULL,
  "UPDATE_RULE" SMALLINT,
  "DELETE_RULE"	SMALLINT,
  "ENABLE_VALIDATE_RELY" SMALLINT NOT NULL,
  "DEFAULT_VALUE" VARCHAR(400),
  CONSTRAINT CONSTRAINTS_PK PRIMARY KEY ("PARENT_TBL_ID", "CONSTRAINT_NAME", "POSITION")
) ;

---
--- Table structure for METASTORE_DB_PROPERTIES
---
CREATE TABLE "METASTORE_DB_PROPERTIES"
(
  "PROPERTY_KEY" VARCHAR(255) NOT NULL,
  "PROPERTY_VALUE" VARCHAR(1000) NOT NULL,
  "DESCRIPTION" VARCHAR(1000)
);


CREATE TABLE "WM_RESOURCEPLAN" (
    "RP_ID" bigint NOT NULL,
    "NAME" character varying(128) NOT NULL,
    "NS" character varying(128),
    "QUERY_PARALLELISM" integer,
    "STATUS" character varying(20) NOT NULL,
    "DEFAULT_POOL_ID" bigint
);

CREATE TABLE "WM_POOL" (
    "POOL_ID" bigint NOT NULL,
    "RP_ID" bigint NOT NULL,
    "PATH" character varying(1024) NOT NULL,
    "ALLOC_FRACTION" double precision,
    "QUERY_PARALLELISM" integer,
    "SCHEDULING_POLICY" character varying(1024)
);

CREATE TABLE "WM_TRIGGER" (
    "TRIGGER_ID" bigint NOT NULL,
    "RP_ID" bigint NOT NULL,
    "NAME" character varying(128) NOT NULL,
    "TRIGGER_EXPRESSION" character varying(1024) DEFAULT NULL::character varying,
    "ACTION_EXPRESSION" character varying(1024) DEFAULT NULL::character varying,
    "IS_IN_UNMANAGED" smallint NOT NULL DEFAULT 0
);

CREATE TABLE "WM_POOL_TO_TRIGGER" (
    "POOL_ID" bigint NOT NULL,
    "TRIGGER_ID" bigint NOT NULL
);

CREATE TABLE "WM_MAPPING" (
    "MAPPING_ID" bigint NOT NULL,
    "RP_ID" bigint NOT NULL,
    "ENTITY_TYPE" character varying(128) NOT NULL,
    "ENTITY_NAME" character varying(128) NOT NULL,
    "POOL_ID" bigint,
    "ORDERING" integer
);

--
-- Name: BUCKETING_COLS_pkey; Type: CONSTRAINT; Schema: public; Owner: hiveuser; Tablespace:
--

ALTER TABLE ONLY "BUCKETING_COLS"
    ADD CONSTRAINT "BUCKETING_COLS_pkey" PRIMARY KEY ("SD_ID", "INTEGER_IDX");


--
-- Name: CDS_pkey; Type: CONSTRAINT; Schema: public; Owner: hiveuser; Tablespace:
--

ALTER TABLE ONLY "CDS"
    ADD CONSTRAINT "CDS_pkey" PRIMARY KEY ("CD_ID");


--
-- Name: COLUMNS_V2_pkey; Type: CONSTRAINT; Schema: public; Owner: hiveuser; Tablespace:
--

ALTER TABLE ONLY "COLUMNS_V2"
    ADD CONSTRAINT "COLUMNS_V2_pkey" PRIMARY KEY ("CD_ID", "COLUMN_NAME");


--
-- Name: DATABASE_PARAMS_pkey; Type: CONSTRAINT; Schema: public; Owner: hiveuser; Tablespace:
--

ALTER TABLE ONLY "DATABASE_PARAMS"
    ADD CONSTRAINT "DATABASE_PARAMS_pkey" PRIMARY KEY ("DB_ID", "PARAM_KEY");


--
-- Name: DBPRIVILEGEINDEX; Type: CONSTRAINT; Schema: public; Owner: hiveuser; Tablespace:
--

ALTER TABLE ONLY "DB_PRIVS"
    ADD CONSTRAINT "DBPRIVILEGEINDEX" UNIQUE ("AUTHORIZER", "DB_ID", "PRINCIPAL_NAME", "PRINCIPAL_TYPE", "DB_PRIV", "GRANTOR", "GRANTOR_TYPE");

--
-- Name: DCPRIVILEGEINDEX; Type: CONSTRAINT; Schema: public; Owner: hiveuser; Tablespace:
--

ALTER TABLE ONLY "DC_PRIVS"
    ADD CONSTRAINT "DCPRIVILEGEINDEX" UNIQUE ("AUTHORIZER", "NAME", "PRINCIPAL_NAME", "PRINCIPAL_TYPE", "DC_PRIV", "GRANTOR", "GRANTOR_TYPE");


--
-- Name: DBS_pkey; Type: CONSTRAINT; Schema: public; Owner: hiveuser; Tablespace:
--

ALTER TABLE ONLY "DBS"
    ADD CONSTRAINT "DBS_pkey" PRIMARY KEY ("DB_ID");


--
-- Name: DB_PRIVS_pkey; Type: CONSTRAINT; Schema: public; Owner: hiveuser; Tablespace:
--

ALTER TABLE ONLY "DB_PRIVS"
    ADD CONSTRAINT "DB_PRIVS_pkey" PRIMARY KEY ("DB_GRANT_ID");


--
-- Name: DC_PRIVS_pkey; Type: CONSTRAINT; Schema: public; Owner: hiveuser; Tablespace:
--

ALTER TABLE ONLY "DC_PRIVS"
    ADD CONSTRAINT "DC_PRIVS_pkey" PRIMARY KEY ("DC_GRANT_ID");


--
-- Name: GLOBALPRIVILEGEINDEX; Type: CONSTRAINT; Schema: public; Owner: hiveuser; Tablespace:
--

ALTER TABLE ONLY "GLOBAL_PRIVS"
    ADD CONSTRAINT "GLOBALPRIVILEGEINDEX" UNIQUE ("AUTHORIZER", "PRINCIPAL_NAME", "PRINCIPAL_TYPE", "USER_PRIV", "GRANTOR", "GRANTOR_TYPE");


--
-- Name: GLOBAL_PRIVS_pkey; Type: CONSTRAINT; Schema: public; Owner: hiveuser; Tablespace:
--

ALTER TABLE ONLY "GLOBAL_PRIVS"
    ADD CONSTRAINT "GLOBAL_PRIVS_pkey" PRIMARY KEY ("USER_GRANT_ID");


--
-- Name: IDXS_pkey; Type: CONSTRAINT; Schema: public; Owner: hiveuser; Tablespace:
--

ALTER TABLE ONLY "IDXS"
    ADD CONSTRAINT "IDXS_pkey" PRIMARY KEY ("INDEX_ID");


--
-- Name: INDEX_PARAMS_pkey; Type: CONSTRAINT; Schema: public; Owner: hiveuser; Tablespace:
--

ALTER TABLE ONLY "INDEX_PARAMS"
    ADD CONSTRAINT "INDEX_PARAMS_pkey" PRIMARY KEY ("INDEX_ID", "PARAM_KEY");


--
-- Name: ONE_ROW_CONSTRAINT; Type: CONSTRAINT; Schema: public; Owner: hiveuser; Tablespace:
--

ALTER TABLE "NOTIFICATION_SEQUENCE"
    ADD CONSTRAINT "ONE_ROW_CONSTRAINT" CHECK ("NNI_ID" = 1);


--
-- Name: NUCLEUS_TABLES_pkey; Type: CONSTRAINT; Schema: public; Owner: hiveuser; Tablespace:
--

ALTER TABLE ONLY "NUCLEUS_TABLES"
    ADD CONSTRAINT "NUCLEUS_TABLES_pkey" PRIMARY KEY ("CLASS_NAME");


--
-- Name: PARTITIONS_pkey; Type: CONSTRAINT; Schema: public; Owner: hiveuser; Tablespace:
--

ALTER TABLE ONLY "PARTITIONS"
    ADD CONSTRAINT "PARTITIONS_pkey" PRIMARY KEY ("PART_ID");


--
-- Name: PARTITION_EVENTS_pkey; Type: CONSTRAINT; Schema: public; Owner: hiveuser; Tablespace:
--

ALTER TABLE ONLY "PARTITION_EVENTS"
    ADD CONSTRAINT "PARTITION_EVENTS_pkey" PRIMARY KEY ("PART_NAME_ID");


--
-- Name: PARTITION_KEYS_pkey; Type: CONSTRAINT; Schema: public; Owner: hiveuser; Tablespace:
--

ALTER TABLE ONLY "PARTITION_KEYS"
    ADD CONSTRAINT "PARTITION_KEYS_pkey" PRIMARY KEY ("TBL_ID", "PKEY_NAME");


--
-- Name: PARTITION_KEY_VALS_pkey; Type: CONSTRAINT; Schema: public; Owner: hiveuser; Tablespace:
--

ALTER TABLE ONLY "PARTITION_KEY_VALS"
    ADD CONSTRAINT "PARTITION_KEY_VALS_pkey" PRIMARY KEY ("PART_ID", "INTEGER_IDX");


--
-- Name: PARTITION_PARAMS_pkey; Type: CONSTRAINT; Schema: public; Owner: hiveuser; Tablespace:
--

ALTER TABLE ONLY "PARTITION_PARAMS"
    ADD CONSTRAINT "PARTITION_PARAMS_pkey" PRIMARY KEY ("PART_ID", "PARAM_KEY");


--
-- Name: PART_COL_PRIVS_pkey; Type: CONSTRAINT; Schema: public; Owner: hiveuser; Tablespace:
--

ALTER TABLE ONLY "PART_COL_PRIVS"
    ADD CONSTRAINT "PART_COL_PRIVS_pkey" PRIMARY KEY ("PART_COLUMN_GRANT_ID");


--
-- Name: PART_PRIVS_pkey; Type: CONSTRAINT; Schema: public; Owner: hiveuser; Tablespace:
--

ALTER TABLE ONLY "PART_PRIVS"
    ADD CONSTRAINT "PART_PRIVS_pkey" PRIMARY KEY ("PART_GRANT_ID");


--
-- Name: ROLEENTITYINDEX; Type: CONSTRAINT; Schema: public; Owner: hiveuser; Tablespace:
--

ALTER TABLE ONLY "ROLES"
    ADD CONSTRAINT "ROLEENTITYINDEX" UNIQUE ("ROLE_NAME");


--
-- Name: ROLES_pkey; Type: CONSTRAINT; Schema: public; Owner: hiveuser; Tablespace:
--

ALTER TABLE ONLY "ROLES"
    ADD CONSTRAINT "ROLES_pkey" PRIMARY KEY ("ROLE_ID");


--
-- Name: ROLE_MAP_pkey; Type: CONSTRAINT; Schema: public; Owner: hiveuser; Tablespace:
--

ALTER TABLE ONLY "ROLE_MAP"
    ADD CONSTRAINT "ROLE_MAP_pkey" PRIMARY KEY ("ROLE_GRANT_ID");


--
-- Name: SDS_pkey; Type: CONSTRAINT; Schema: public; Owner: hiveuser; Tablespace:
--

ALTER TABLE ONLY "SDS"
    ADD CONSTRAINT "SDS_pkey" PRIMARY KEY ("SD_ID");


--
-- Name: SD_PARAMS_pkey; Type: CONSTRAINT; Schema: public; Owner: hiveuser; Tablespace:
--

ALTER TABLE ONLY "SD_PARAMS"
    ADD CONSTRAINT "SD_PARAMS_pkey" PRIMARY KEY ("SD_ID", "PARAM_KEY");


--
-- Name: SEQUENCE_TABLE_pkey; Type: CONSTRAINT; Schema: public; Owner: hiveuser; Tablespace:
--

ALTER TABLE ONLY "SEQUENCE_TABLE"
    ADD CONSTRAINT "SEQUENCE_TABLE_pkey" PRIMARY KEY ("SEQUENCE_NAME");


--
-- Name: SERDES_pkey; Type: CONSTRAINT; Schema: public; Owner: hiveuser; Tablespace:
--

ALTER TABLE ONLY "SERDES"
    ADD CONSTRAINT "SERDES_pkey" PRIMARY KEY ("SERDE_ID");


--
-- Name: SERDE_PARAMS_pkey; Type: CONSTRAINT; Schema: public; Owner: hiveuser; Tablespace:
--

ALTER TABLE ONLY "SERDE_PARAMS"
    ADD CONSTRAINT "SERDE_PARAMS_pkey" PRIMARY KEY ("SERDE_ID", "PARAM_KEY");


--
-- Name: SORT_COLS_pkey; Type: CONSTRAINT; Schema: public; Owner: hiveuser; Tablespace:
--

ALTER TABLE ONLY "SORT_COLS"
    ADD CONSTRAINT "SORT_COLS_pkey" PRIMARY KEY ("SD_ID", "INTEGER_IDX");


--
-- Name: TABLE_PARAMS_pkey; Type: CONSTRAINT; Schema: public; Owner: hiveuser; Tablespace:
--

ALTER TABLE ONLY "TABLE_PARAMS"
    ADD CONSTRAINT "TABLE_PARAMS_pkey" PRIMARY KEY ("TBL_ID", "PARAM_KEY");


--
-- Name: TBLS_pkey; Type: CONSTRAINT; Schema: public; Owner: hiveuser; Tablespace:
--

ALTER TABLE ONLY "TBLS"
    ADD CONSTRAINT "TBLS_pkey" PRIMARY KEY ("TBL_ID");


--
-- Name: TBL_COL_PRIVS_pkey; Type: CONSTRAINT; Schema: public; Owner: hiveuser; Tablespace:
--

ALTER TABLE ONLY "TBL_COL_PRIVS"
    ADD CONSTRAINT "TBL_COL_PRIVS_pkey" PRIMARY KEY ("TBL_COLUMN_GRANT_ID");


--
-- Name: TBL_PRIVS_pkey; Type: CONSTRAINT; Schema: public; Owner: hiveuser; Tablespace:
--

ALTER TABLE ONLY "TBL_PRIVS"
    ADD CONSTRAINT "TBL_PRIVS_pkey" PRIMARY KEY ("TBL_GRANT_ID");


--
-- Name: TYPES_pkey; Type: CONSTRAINT; Schema: public; Owner: hiveuser; Tablespace:
--

ALTER TABLE ONLY "TYPES"
    ADD CONSTRAINT "TYPES_pkey" PRIMARY KEY ("TYPES_ID");


--
-- Name: TYPE_FIELDS_pkey; Type: CONSTRAINT; Schema: public; Owner: hiveuser; Tablespace:
--

ALTER TABLE ONLY "TYPE_FIELDS"
    ADD CONSTRAINT "TYPE_FIELDS_pkey" PRIMARY KEY ("TYPE_NAME", "FIELD_NAME");

ALTER TABLE ONLY "SKEWED_STRING_LIST"
    ADD CONSTRAINT "SKEWED_STRING_LIST_pkey" PRIMARY KEY ("STRING_LIST_ID");

ALTER TABLE ONLY "SKEWED_STRING_LIST_VALUES"
    ADD CONSTRAINT "SKEWED_STRING_LIST_VALUES_pkey" PRIMARY KEY ("STRING_LIST_ID", "INTEGER_IDX");


ALTER TABLE ONLY "SKEWED_COL_NAMES"
    ADD CONSTRAINT "SKEWED_COL_NAMES_pkey" PRIMARY KEY ("SD_ID", "INTEGER_IDX");

ALTER TABLE ONLY "SKEWED_COL_VALUE_LOC_MAP"
    ADD CONSTRAINT "SKEWED_COL_VALUE_LOC_MAP_pkey" PRIMARY KEY ("SD_ID", "STRING_LIST_ID_KID");

ALTER TABLE ONLY "SKEWED_VALUES"
    ADD CONSTRAINT "SKEWED_VALUES_pkey" PRIMARY KEY ("SD_ID_OID", "INTEGER_IDX");

--
-- Name: TAB_COL_STATS_pkey; Type: CONSTRAINT; Schema: public; Owner: hiveuser; Tablespace:
--
ALTER TABLE ONLY "TAB_COL_STATS" ADD CONSTRAINT "TAB_COL_STATS_pkey" PRIMARY KEY("CS_ID");

--
-- Name: PART_COL_STATS_pkey; Type: CONSTRAINT; Schema: public; Owner: hiveuser; Tablespace:
--
ALTER TABLE ONLY "PART_COL_STATS" ADD CONSTRAINT "PART_COL_STATS_pkey" PRIMARY KEY("CS_ID");

--
-- Name: UNIQUEINDEX; Type: CONSTRAINT; Schema: public; Owner: hiveuser; Tablespace:
--

ALTER TABLE ONLY "IDXS"
    ADD CONSTRAINT "UNIQUEINDEX" UNIQUE ("INDEX_NAME", "ORIG_TBL_ID");


--
-- Name: UNIQUEPARTITION; Type: CONSTRAINT; Schema: public; Owner: hiveuser; Tablespace:
--

ALTER TABLE ONLY "PARTITIONS"
    ADD CONSTRAINT "UNIQUEPARTITION" UNIQUE ("PART_NAME", "TBL_ID");


--
-- Name: UNIQUETABLE; Type: CONSTRAINT; Schema: public; Owner: hiveuser; Tablespace:
--

ALTER TABLE ONLY "TBLS"
    ADD CONSTRAINT "UNIQUETABLE" UNIQUE ("TBL_NAME", "DB_ID");


--
-- Name: UNIQUE_DATABASE; Type: CONSTRAINT; Schema: public; Owner: hiveuser; Tablespace:
--

ALTER TABLE ONLY "DBS"
    ADD CONSTRAINT "UNIQUE_DATABASE" UNIQUE ("NAME", "CTLG_NAME");


--
-- Name: UNIQUE_TYPE; Type: CONSTRAINT; Schema: public; Owner: hiveuser; Tablespace:
--

ALTER TABLE ONLY "TYPES"
    ADD CONSTRAINT "UNIQUE_TYPE" UNIQUE ("TYPE_NAME");


--
-- Name: USERROLEMAPINDEX; Type: CONSTRAINT; Schema: public; Owner: hiveuser; Tablespace:
--

ALTER TABLE ONLY "ROLE_MAP"
    ADD CONSTRAINT "USERROLEMAPINDEX" UNIQUE ("PRINCIPAL_NAME", "ROLE_ID", "GRANTOR", "GRANTOR_TYPE");

ALTER TABLE ONLY "METASTORE_DB_PROPERTIES"
    ADD CONSTRAINT "PROPERTY_KEY_PK" PRIMARY KEY ("PROPERTY_KEY");


-- Resource plan: Primary key and unique key constraints.
ALTER TABLE ONLY "WM_RESOURCEPLAN"
    ADD CONSTRAINT "WM_RESOURCEPLAN_pkey" PRIMARY KEY ("RP_ID");

ALTER TABLE ONLY "WM_RESOURCEPLAN"
    ADD CONSTRAINT "UNIQUE_WM_RESOURCEPLAN" UNIQUE ("NS", "NAME");

ALTER TABLE ONLY "WM_POOL"
    ADD CONSTRAINT "WM_POOL_pkey" PRIMARY KEY ("POOL_ID");

ALTER TABLE ONLY "WM_POOL"
    ADD CONSTRAINT "UNIQUE_WM_POOL" UNIQUE ("RP_ID", "PATH");

ALTER TABLE ONLY "WM_TRIGGER"
    ADD CONSTRAINT "WM_TRIGGER_pkey" PRIMARY KEY ("TRIGGER_ID");

ALTER TABLE ONLY "WM_TRIGGER"
    ADD CONSTRAINT "UNIQUE_WM_TRIGGER" UNIQUE ("RP_ID", "NAME");

ALTER TABLE ONLY "WM_POOL_TO_TRIGGER"
    ADD CONSTRAINT "WM_POOL_TO_TRIGGER_pkey" PRIMARY KEY ("POOL_ID", "TRIGGER_ID");

ALTER TABLE ONLY "WM_MAPPING"
    ADD CONSTRAINT "WM_MAPPING_pkey" PRIMARY KEY ("MAPPING_ID");

ALTER TABLE ONLY "WM_MAPPING"
    ADD CONSTRAINT "UNIQUE_WM_MAPPING" UNIQUE ("RP_ID", "ENTITY_TYPE", "ENTITY_NAME");

--
-- Name: BUCKETING_COLS_N49; Type: INDEX; Schema: public; Owner: hiveuser; Tablespace:
--

CREATE INDEX "BUCKETING_COLS_N49" ON "BUCKETING_COLS" USING btree ("SD_ID");


--
-- Name: DATABASE_PARAMS_N49; Type: INDEX; Schema: public; Owner: hiveuser; Tablespace:
--

CREATE INDEX "DATABASE_PARAMS_N49" ON "DATABASE_PARAMS" USING btree ("DB_ID");


--
-- Name: DB_PRIVS_N49; Type: INDEX; Schema: public; Owner: hiveuser; Tablespace:
--

CREATE INDEX "DB_PRIVS_N49" ON "DB_PRIVS" USING btree ("DB_ID");


--
-- Name: DC_PRIVS_N49; Type: INDEX; Schema: public; Owner: hiveuser; Tablespace:
--

CREATE INDEX "DC_PRIVS_N49" ON "DC_PRIVS" USING btree ("NAME");


--
-- Name: IDXS_N49; Type: INDEX; Schema: public; Owner: hiveuser; Tablespace:
--

CREATE INDEX "IDXS_N49" ON "IDXS" USING btree ("ORIG_TBL_ID");


--
-- Name: IDXS_N50; Type: INDEX; Schema: public; Owner: hiveuser; Tablespace:
--

CREATE INDEX "IDXS_N50" ON "IDXS" USING btree ("INDEX_TBL_ID");


--
-- Name: IDXS_N51; Type: INDEX; Schema: public; Owner: hiveuser; Tablespace:
--

CREATE INDEX "IDXS_N51" ON "IDXS" USING btree ("SD_ID");


--
-- Name: INDEX_PARAMS_N49; Type: INDEX; Schema: public; Owner: hiveuser; Tablespace:
--

CREATE INDEX "INDEX_PARAMS_N49" ON "INDEX_PARAMS" USING btree ("INDEX_ID");


--
-- Name: PARTITIONCOLUMNPRIVILEGEINDEX; Type: INDEX; Schema: public; Owner: hiveuser; Tablespace:
--

CREATE INDEX "PARTITIONCOLUMNPRIVILEGEINDEX" ON "PART_COL_PRIVS" USING btree ("AUTHORIZER", "PART_ID", "COLUMN_NAME", "PRINCIPAL_NAME", "PRINCIPAL_TYPE", "PART_COL_PRIV", "GRANTOR", "GRANTOR_TYPE");


--
-- Name: PARTITIONEVENTINDEX; Type: INDEX; Schema: public; Owner: hiveuser; Tablespace:
--

CREATE INDEX "PARTITIONEVENTINDEX" ON "PARTITION_EVENTS" USING btree ("PARTITION_NAME");


--
-- Name: PARTITIONS_N49; Type: INDEX; Schema: public; Owner: hiveuser; Tablespace:
--

CREATE INDEX "PARTITIONS_N49" ON "PARTITIONS" USING btree ("TBL_ID");


--
-- Name: PARTITIONS_N50; Type: INDEX; Schema: public; Owner: hiveuser; Tablespace:
--

CREATE INDEX "PARTITIONS_N50" ON "PARTITIONS" USING btree ("SD_ID");


--
-- Name: PARTITION_KEYS_N49; Type: INDEX; Schema: public; Owner: hiveuser; Tablespace:
--

CREATE INDEX "PARTITION_KEYS_N49" ON "PARTITION_KEYS" USING btree ("TBL_ID");


--
-- Name: PARTITION_KEY_VALS_N49; Type: INDEX; Schema: public; Owner: hiveuser; Tablespace:
--

CREATE INDEX "PARTITION_KEY_VALS_N49" ON "PARTITION_KEY_VALS" USING btree ("PART_ID");


--
-- Name: PARTITION_PARAMS_N49; Type: INDEX; Schema: public; Owner: hiveuser; Tablespace:
--

CREATE INDEX "PARTITION_PARAMS_N49" ON "PARTITION_PARAMS" USING btree ("PART_ID");


--
-- Name: PARTPRIVILEGEINDEX; Type: INDEX; Schema: public; Owner: hiveuser; Tablespace:
--

CREATE INDEX "PARTPRIVILEGEINDEX" ON "PART_PRIVS" USING btree ("AUTHORIZER", "PART_ID", "PRINCIPAL_NAME", "PRINCIPAL_TYPE", "PART_PRIV", "GRANTOR", "GRANTOR_TYPE");


--
-- Name: PART_COL_PRIVS_N49; Type: INDEX; Schema: public; Owner: hiveuser; Tablespace:
--

CREATE INDEX "PART_COL_PRIVS_N49" ON "PART_COL_PRIVS" USING btree ("PART_ID");


--
-- Name: PART_PRIVS_N49; Type: INDEX; Schema: public; Owner: hiveuser; Tablespace:
--

CREATE INDEX "PART_PRIVS_N49" ON "PART_PRIVS" USING btree ("PART_ID");


--
-- Name: PCS_STATS_IDX; Type: INDEX; Schema: public; Owner: hiveuser; Tablespace:
--

CREATE INDEX "PCS_STATS_IDX" ON "PART_COL_STATS" USING btree ("CAT_NAME", "DB_NAME","TABLE_NAME","COLUMN_NAME","PARTITION_NAME");


--
-- Name: ROLE_MAP_N49; Type: INDEX; Schema: public; Owner: hiveuser; Tablespace:
--

CREATE INDEX "ROLE_MAP_N49" ON "ROLE_MAP" USING btree ("ROLE_ID");


--
-- Name: SDS_N49; Type: INDEX; Schema: public; Owner: hiveuser; Tablespace:
--

CREATE INDEX "SDS_N49" ON "SDS" USING btree ("SERDE_ID");

--
-- Name: SDS_N50; Type: INDEX; Schema: public; Owner: hiveuser; Tablespace:
--

CREATE INDEX "SDS_N50" ON "SDS" USING btree ("CD_ID");

--
-- Name: SD_PARAMS_N49; Type: INDEX; Schema: public; Owner: hiveuser; Tablespace:
--

CREATE INDEX "SD_PARAMS_N49" ON "SD_PARAMS" USING btree ("SD_ID");


--
-- Name: SERDE_PARAMS_N49; Type: INDEX; Schema: public; Owner: hiveuser; Tablespace:
--

CREATE INDEX "SERDE_PARAMS_N49" ON "SERDE_PARAMS" USING btree ("SERDE_ID");


--
-- Name: SORT_COLS_N49; Type: INDEX; Schema: public; Owner: hiveuser; Tablespace:
--

CREATE INDEX "SORT_COLS_N49" ON "SORT_COLS" USING btree ("SD_ID");


--
-- Name: TABLECOLUMNPRIVILEGEINDEX; Type: INDEX; Schema: public; Owner: hiveuser; Tablespace:
--

CREATE INDEX "TABLECOLUMNPRIVILEGEINDEX" ON "TBL_COL_PRIVS" USING btree ("AUTHORIZER", "TBL_ID", "COLUMN_NAME", "PRINCIPAL_NAME", "PRINCIPAL_TYPE", "TBL_COL_PRIV", "GRANTOR", "GRANTOR_TYPE");


--
-- Name: TABLEPRIVILEGEINDEX; Type: INDEX; Schema: public; Owner: hiveuser; Tablespace:
--

CREATE INDEX "TABLEPRIVILEGEINDEX" ON "TBL_PRIVS" USING btree ("AUTHORIZER", "TBL_ID", "PRINCIPAL_NAME", "PRINCIPAL_TYPE", "TBL_PRIV", "GRANTOR", "GRANTOR_TYPE");


--
-- Name: TABLE_PARAMS_N49; Type: INDEX; Schema: public; Owner: hiveuser; Tablespace:
--

CREATE INDEX "TABLE_PARAMS_N49" ON "TABLE_PARAMS" USING btree ("TBL_ID");


--
-- Name: TBLS_N49; Type: INDEX; Schema: public; Owner: hiveuser; Tablespace:
--

CREATE INDEX "TBLS_N49" ON "TBLS" USING btree ("DB_ID");


--
-- Name: TBLS_N50; Type: INDEX; Schema: public; Owner: hiveuser; Tablespace:
--

CREATE INDEX "TBLS_N50" ON "TBLS" USING btree ("SD_ID");


--
-- Name: TBL_COL_PRIVS_N49; Type: INDEX; Schema: public; Owner: hiveuser; Tablespace:
--

CREATE INDEX "TBL_COL_PRIVS_N49" ON "TBL_COL_PRIVS" USING btree ("TBL_ID");

--
-- Name: TAB_COL_STATS_IDX; Type: INDEX; Schema: public; Owner: hiveuser; Tablespace:
--

CREATE INDEX "TAB_COL_STATS_IDX" ON "TAB_COL_STATS" USING btree ("CAT_NAME", "DB_NAME","TABLE_NAME","COLUMN_NAME");

--
-- Name: TBL_PRIVS_N49; Type: INDEX; Schema: public; Owner: hiveuser; Tablespace:
--

CREATE INDEX "TBL_PRIVS_N49" ON "TBL_PRIVS" USING btree ("TBL_ID");


--
-- Name: TYPE_FIELDS_N49; Type: INDEX; Schema: public; Owner: hiveuser; Tablespace:
--

CREATE INDEX "TYPE_FIELDS_N49" ON "TYPE_FIELDS" USING btree ("TYPE_NAME");

--
-- Name: TAB_COL_STATS_N49; Type: INDEX; Schema: public; Owner: hiveuser; Tablespace:
--

CREATE INDEX "TAB_COL_STATS_N49" ON "TAB_COL_STATS" USING btree ("TBL_ID");

--
-- Name: PART_COL_STATS_N49; Type: INDEX; Schema: public; Owner: hiveuser; Tablespace:
--

CREATE INDEX "PART_COL_STATS_N49" ON "PART_COL_STATS" USING btree ("PART_ID");

--
-- Name: UNIQUEFUNCTION; Type: INDEX; Schema: public; Owner: hiveuser; Tablespace:
--

CREATE UNIQUE INDEX "UNIQUEFUNCTION" ON "FUNCS" ("FUNC_NAME", "DB_ID");

--
-- Name: FUNCS_N49; Type: INDEX; Schema: public; Owner: hiveuser; Tablespace:
--

CREATE INDEX "FUNCS_N49" ON "FUNCS" ("DB_ID");

--
-- Name: FUNC_RU_N49; Type: INDEX; Schema: public; Owner: hiveuser; Tablespace:
--

CREATE INDEX "FUNC_RU_N49" ON "FUNC_RU" ("FUNC_ID");

CREATE INDEX "CONSTRAINTS_PARENT_TBLID_INDEX" ON "KEY_CONSTRAINTS" USING BTREE ("PARENT_TBL_ID");

CREATE INDEX "CONSTRAINTS_CONSTRAINT_TYPE_INDEX" ON "KEY_CONSTRAINTS" USING BTREE ("CONSTRAINT_TYPE");

ALTER TABLE ONLY "SKEWED_STRING_LIST_VALUES"
    ADD CONSTRAINT "SKEWED_STRING_LIST_VALUES_fkey" FOREIGN KEY ("STRING_LIST_ID") REFERENCES "SKEWED_STRING_LIST"("STRING_LIST_ID") DEFERRABLE;


ALTER TABLE ONLY "SKEWED_COL_NAMES"
    ADD CONSTRAINT "SKEWED_COL_NAMES_fkey" FOREIGN KEY ("SD_ID") REFERENCES "SDS"("SD_ID") DEFERRABLE;


ALTER TABLE ONLY "SKEWED_COL_VALUE_LOC_MAP"
    ADD CONSTRAINT "SKEWED_COL_VALUE_LOC_MAP_fkey1" FOREIGN KEY ("SD_ID") REFERENCES "SDS"("SD_ID") DEFERRABLE;

ALTER TABLE ONLY "SKEWED_COL_VALUE_LOC_MAP"
    ADD CONSTRAINT "SKEWED_COL_VALUE_LOC_MAP_fkey2" FOREIGN KEY ("STRING_LIST_ID_KID") REFERENCES "SKEWED_STRING_LIST"("STRING_LIST_ID") DEFERRABLE;

ALTER TABLE ONLY "SKEWED_VALUES"
    ADD CONSTRAINT "SKEWED_VALUES_fkey1" FOREIGN KEY ("STRING_LIST_ID_EID") REFERENCES "SKEWED_STRING_LIST"("STRING_LIST_ID") DEFERRABLE;

ALTER TABLE ONLY "SKEWED_VALUES"
    ADD CONSTRAINT "SKEWED_VALUES_fkey2" FOREIGN KEY ("SD_ID_OID") REFERENCES "SDS"("SD_ID") DEFERRABLE;


--
-- Name: BUCKETING_COLS_SD_ID_fkey; Type: FK CONSTRAINT; Schema: public; Owner: hiveuser
--

ALTER TABLE ONLY "BUCKETING_COLS"
    ADD CONSTRAINT "BUCKETING_COLS_SD_ID_fkey" FOREIGN KEY ("SD_ID") REFERENCES "SDS"("SD_ID") DEFERRABLE;


--
-- Name: COLUMNS_V2_CD_ID_fkey; Type: FK CONSTRAINT; Schema: public; Owner: hiveuser
--

ALTER TABLE ONLY "COLUMNS_V2"
    ADD CONSTRAINT "COLUMNS_V2_CD_ID_fkey" FOREIGN KEY ("CD_ID") REFERENCES "CDS"("CD_ID") DEFERRABLE;


--
-- Name: DATABASE_PARAMS_DB_ID_fkey; Type: FK CONSTRAINT; Schema: public; Owner: hiveuser
--

ALTER TABLE ONLY "DATABASE_PARAMS"
    ADD CONSTRAINT "DATABASE_PARAMS_DB_ID_fkey" FOREIGN KEY ("DB_ID") REFERENCES "DBS"("DB_ID") DEFERRABLE;


--
-- Name: DB_PRIVS_DB_ID_fkey; Type: FK CONSTRAINT; Schema: public; Owner: hiveuser
--

ALTER TABLE ONLY "DB_PRIVS"
    ADD CONSTRAINT "DB_PRIVS_DB_ID_fkey" FOREIGN KEY ("DB_ID") REFERENCES "DBS"("DB_ID") DEFERRABLE;


--
-- Name: IDXS_INDEX_TBL_ID_fkey; Type: FK CONSTRAINT; Schema: public; Owner: hiveuser
--

ALTER TABLE ONLY "IDXS"
    ADD CONSTRAINT "IDXS_INDEX_TBL_ID_fkey" FOREIGN KEY ("INDEX_TBL_ID") REFERENCES "TBLS"("TBL_ID") DEFERRABLE;


--
-- Name: IDXS_ORIG_TBL_ID_fkey; Type: FK CONSTRAINT; Schema: public; Owner: hiveuser
--

ALTER TABLE ONLY "IDXS"
    ADD CONSTRAINT "IDXS_ORIG_TBL_ID_fkey" FOREIGN KEY ("ORIG_TBL_ID") REFERENCES "TBLS"("TBL_ID") DEFERRABLE;


--
-- Name: IDXS_SD_ID_fkey; Type: FK CONSTRAINT; Schema: public; Owner: hiveuser
--

ALTER TABLE ONLY "IDXS"
    ADD CONSTRAINT "IDXS_SD_ID_fkey" FOREIGN KEY ("SD_ID") REFERENCES "SDS"("SD_ID") DEFERRABLE;


--
-- Name: INDEX_PARAMS_INDEX_ID_fkey; Type: FK CONSTRAINT; Schema: public; Owner: hiveuser
--

ALTER TABLE ONLY "INDEX_PARAMS"
    ADD CONSTRAINT "INDEX_PARAMS_INDEX_ID_fkey" FOREIGN KEY ("INDEX_ID") REFERENCES "IDXS"("INDEX_ID") DEFERRABLE;


--
-- Name: PARTITIONS_SD_ID_fkey; Type: FK CONSTRAINT; Schema: public; Owner: hiveuser
--

ALTER TABLE ONLY "PARTITIONS"
    ADD CONSTRAINT "PARTITIONS_SD_ID_fkey" FOREIGN KEY ("SD_ID") REFERENCES "SDS"("SD_ID") DEFERRABLE;


--
-- Name: PARTITIONS_TBL_ID_fkey; Type: FK CONSTRAINT; Schema: public; Owner: hiveuser
--

ALTER TABLE ONLY "PARTITIONS"
    ADD CONSTRAINT "PARTITIONS_TBL_ID_fkey" FOREIGN KEY ("TBL_ID") REFERENCES "TBLS"("TBL_ID") DEFERRABLE;


--
-- Name: PARTITION_KEYS_TBL_ID_fkey; Type: FK CONSTRAINT; Schema: public; Owner: hiveuser
--

ALTER TABLE ONLY "PARTITION_KEYS"
    ADD CONSTRAINT "PARTITION_KEYS_TBL_ID_fkey" FOREIGN KEY ("TBL_ID") REFERENCES "TBLS"("TBL_ID") DEFERRABLE;


--
-- Name: PARTITION_KEY_VALS_PART_ID_fkey; Type: FK CONSTRAINT; Schema: public; Owner: hiveuser
--

ALTER TABLE ONLY "PARTITION_KEY_VALS"
    ADD CONSTRAINT "PARTITION_KEY_VALS_PART_ID_fkey" FOREIGN KEY ("PART_ID") REFERENCES "PARTITIONS"("PART_ID") DEFERRABLE;


--
-- Name: PARTITION_PARAMS_PART_ID_fkey; Type: FK CONSTRAINT; Schema: public; Owner: hiveuser
--

ALTER TABLE ONLY "PARTITION_PARAMS"
    ADD CONSTRAINT "PARTITION_PARAMS_PART_ID_fkey" FOREIGN KEY ("PART_ID") REFERENCES "PARTITIONS"("PART_ID") DEFERRABLE;


--
-- Name: PART_COL_PRIVS_PART_ID_fkey; Type: FK CONSTRAINT; Schema: public; Owner: hiveuser
--

ALTER TABLE ONLY "PART_COL_PRIVS"
    ADD CONSTRAINT "PART_COL_PRIVS_PART_ID_fkey" FOREIGN KEY ("PART_ID") REFERENCES "PARTITIONS"("PART_ID") DEFERRABLE;


--
-- Name: PART_PRIVS_PART_ID_fkey; Type: FK CONSTRAINT; Schema: public; Owner: hiveuser
--

ALTER TABLE ONLY "PART_PRIVS"
    ADD CONSTRAINT "PART_PRIVS_PART_ID_fkey" FOREIGN KEY ("PART_ID") REFERENCES "PARTITIONS"("PART_ID") DEFERRABLE;


--
-- Name: ROLE_MAP_ROLE_ID_fkey; Type: FK CONSTRAINT; Schema: public; Owner: hiveuser
--

ALTER TABLE ONLY "ROLE_MAP"
    ADD CONSTRAINT "ROLE_MAP_ROLE_ID_fkey" FOREIGN KEY ("ROLE_ID") REFERENCES "ROLES"("ROLE_ID") DEFERRABLE;


--
-- Name: SDS_CD_ID_fkey; Type: FK CONSTRAINT; Schema: public; Owner: hiveuser
--

ALTER TABLE ONLY "SDS"
    ADD CONSTRAINT "SDS_CD_ID_fkey" FOREIGN KEY ("CD_ID") REFERENCES "CDS"("CD_ID") DEFERRABLE;


--
-- Name: SDS_SERDE_ID_fkey; Type: FK CONSTRAINT; Schema: public; Owner: hiveuser
--

ALTER TABLE ONLY "SDS"
    ADD CONSTRAINT "SDS_SERDE_ID_fkey" FOREIGN KEY ("SERDE_ID") REFERENCES "SERDES"("SERDE_ID") DEFERRABLE;


--
-- Name: SD_PARAMS_SD_ID_fkey; Type: FK CONSTRAINT; Schema: public; Owner: hiveuser
--

ALTER TABLE ONLY "SD_PARAMS"
    ADD CONSTRAINT "SD_PARAMS_SD_ID_fkey" FOREIGN KEY ("SD_ID") REFERENCES "SDS"("SD_ID") DEFERRABLE;


--
-- Name: SERDE_PARAMS_SERDE_ID_fkey; Type: FK CONSTRAINT; Schema: public; Owner: hiveuser
--

ALTER TABLE ONLY "SERDE_PARAMS"
    ADD CONSTRAINT "SERDE_PARAMS_SERDE_ID_fkey" FOREIGN KEY ("SERDE_ID") REFERENCES "SERDES"("SERDE_ID") DEFERRABLE;


--
-- Name: SORT_COLS_SD_ID_fkey; Type: FK CONSTRAINT; Schema: public; Owner: hiveuser
--

ALTER TABLE ONLY "SORT_COLS"
    ADD CONSTRAINT "SORT_COLS_SD_ID_fkey" FOREIGN KEY ("SD_ID") REFERENCES "SDS"("SD_ID") DEFERRABLE;


--
-- Name: TABLE_PARAMS_TBL_ID_fkey; Type: FK CONSTRAINT; Schema: public; Owner: hiveuser
--

ALTER TABLE ONLY "TABLE_PARAMS"
    ADD CONSTRAINT "TABLE_PARAMS_TBL_ID_fkey" FOREIGN KEY ("TBL_ID") REFERENCES "TBLS"("TBL_ID") DEFERRABLE;


--
-- Name: TBLS_DB_ID_fkey; Type: FK CONSTRAINT; Schema: public; Owner: hiveuser
--

ALTER TABLE ONLY "TBLS"
    ADD CONSTRAINT "TBLS_DB_ID_fkey" FOREIGN KEY ("DB_ID") REFERENCES "DBS"("DB_ID") DEFERRABLE;


--
-- Name: TBLS_SD_ID_fkey; Type: FK CONSTRAINT; Schema: public; Owner: hiveuser
--

ALTER TABLE ONLY "TBLS"
    ADD CONSTRAINT "TBLS_SD_ID_fkey" FOREIGN KEY ("SD_ID") REFERENCES "SDS"("SD_ID") DEFERRABLE;


--
-- Name: TBL_COL_PRIVS_TBL_ID_fkey; Type: FK CONSTRAINT; Schema: public; Owner: hiveuser
--

ALTER TABLE ONLY "TBL_COL_PRIVS"
    ADD CONSTRAINT "TBL_COL_PRIVS_TBL_ID_fkey" FOREIGN KEY ("TBL_ID") REFERENCES "TBLS"("TBL_ID") DEFERRABLE;


--
-- Name: TBL_PRIVS_TBL_ID_fkey; Type: FK CONSTRAINT; Schema: public; Owner: hiveuser
--

ALTER TABLE ONLY "TBL_PRIVS"
    ADD CONSTRAINT "TBL_PRIVS_TBL_ID_fkey" FOREIGN KEY ("TBL_ID") REFERENCES "TBLS"("TBL_ID") DEFERRABLE;


--
-- Name: TYPE_FIELDS_TYPE_NAME_fkey; Type: FK CONSTRAINT; Schema: public; Owner: hiveuser
--

ALTER TABLE ONLY "TYPE_FIELDS"
    ADD CONSTRAINT "TYPE_FIELDS_TYPE_NAME_fkey" FOREIGN KEY ("TYPE_NAME") REFERENCES "TYPES"("TYPES_ID") DEFERRABLE;

--
-- Name: TAB_COL_STATS_fkey; Type: FK CONSTRAINT; Schema: public; Owner: hiveuser
--
ALTER TABLE ONLY "TAB_COL_STATS" ADD CONSTRAINT "TAB_COL_STATS_fkey" FOREIGN KEY("TBL_ID") REFERENCES "TBLS"("TBL_ID") DEFERRABLE;


--
-- Name: PART_COL_STATS_fkey; Type: FK CONSTRAINT; Schema: public; Owner: hiveuser
--
ALTER TABLE ONLY "PART_COL_STATS" ADD CONSTRAINT "PART_COL_STATS_fkey" FOREIGN KEY("PART_ID") REFERENCES "PARTITIONS"("PART_ID") DEFERRABLE;

ALTER TABLE "DBS" ADD CONSTRAINT "DBS_FK1" FOREIGN KEY ("CTLG_NAME") REFERENCES "CTLGS" ("NAME");

ALTER TABLE ONLY "VERSION" ADD CONSTRAINT "VERSION_pkey" PRIMARY KEY ("VER_ID");

-- Name: FUNCS_FK1; Type: FK CONSTRAINT; Schema: public; Owner: hiveuser
ALTER TABLE ONLY "FUNCS"
    ADD CONSTRAINT "FUNCS_FK1" FOREIGN KEY ("DB_ID") REFERENCES "DBS" ("DB_ID") DEFERRABLE;

-- Name: FUNC_RU_FK1; Type: FK CONSTRAINT; Schema: public; Owner: hiveuser
ALTER TABLE ONLY "FUNC_RU"
    ADD CONSTRAINT "FUNC_RU_FK1" FOREIGN KEY ("FUNC_ID") REFERENCES "FUNCS" ("FUNC_ID") DEFERRABLE;

-- Resource plan FK constraints.

ALTER TABLE ONLY "WM_POOL"
    ADD CONSTRAINT "WM_POOL_FK1" FOREIGN KEY ("RP_ID") REFERENCES "WM_RESOURCEPLAN" ("RP_ID") DEFERRABLE;

ALTER TABLE ONLY "WM_RESOURCEPLAN"
    ADD CONSTRAINT "WM_RESOURCEPLAN_FK1" FOREIGN KEY ("DEFAULT_POOL_ID") REFERENCES "WM_POOL" ("POOL_ID") DEFERRABLE;

ALTER TABLE ONLY "WM_TRIGGER"
    ADD CONSTRAINT "WM_TRIGGER_FK1" FOREIGN KEY ("RP_ID") REFERENCES "WM_RESOURCEPLAN" ("RP_ID") DEFERRABLE;

ALTER TABLE ONLY "WM_POOL_TO_TRIGGER"
    ADD CONSTRAINT "WM_POOL_TO_TRIGGER_FK1" FOREIGN KEY ("POOL_ID") REFERENCES "WM_POOL" ("POOL_ID") DEFERRABLE;

ALTER TABLE ONLY "WM_POOL_TO_TRIGGER"
    ADD CONSTRAINT "WM_POOL_TO_TRIGGER_FK2" FOREIGN KEY ("TRIGGER_ID") REFERENCES "WM_TRIGGER" ("TRIGGER_ID") DEFERRABLE;

ALTER TABLE ONLY "WM_MAPPING"
    ADD CONSTRAINT "WM_MAPPING_FK1" FOREIGN KEY ("RP_ID") REFERENCES "WM_RESOURCEPLAN" ("RP_ID") DEFERRABLE;

ALTER TABLE ONLY "WM_MAPPING"
    ADD CONSTRAINT "WM_MAPPING_FK2" FOREIGN KEY ("POOL_ID") REFERENCES "WM_POOL" ("POOL_ID") DEFERRABLE;

ALTER TABLE ONLY "MV_CREATION_METADATA"
    ADD CONSTRAINT "MV_CREATION_METADATA_PK" PRIMARY KEY ("MV_CREATION_METADATA_ID");

CREATE INDEX "MV_UNIQUE_TABLE"
    ON "MV_CREATION_METADATA" USING btree ("TBL_NAME", "DB_NAME");

ALTER TABLE ONLY "MV_TABLES_USED"
    ADD CONSTRAINT "MV_TABLES_USED_FK1" FOREIGN KEY ("MV_CREATION_METADATA_ID") REFERENCES "MV_CREATION_METADATA" ("MV_CREATION_METADATA_ID") DEFERRABLE;

ALTER TABLE ONLY "MV_TABLES_USED"
    ADD CONSTRAINT "MV_TABLES_USED_FK2" FOREIGN KEY ("TBL_ID") REFERENCES "TBLS" ("TBL_ID") DEFERRABLE;

--
-- Name: public; Type: ACL; Schema: -; Owner: hiveuser
--

REVOKE ALL ON SCHEMA public FROM PUBLIC;
GRANT ALL ON SCHEMA public TO PUBLIC;

--
-- PostgreSQL database dump complete
--

------------------------------
-- Transaction and lock tables
------------------------------
CREATE TABLE "TXNS" (
  "TXN_ID" bigserial PRIMARY KEY,
  "TXN_STATE" char(1) NOT NULL,
  "TXN_STARTED" bigint NOT NULL,
  "TXN_LAST_HEARTBEAT" bigint NOT NULL,
  "TXN_USER" varchar(128) NOT NULL,
  "TXN_HOST" varchar(128) NOT NULL,
  "TXN_AGENT_INFO" varchar(128),
  "TXN_META_INFO" varchar(128),
  "TXN_HEARTBEAT_COUNT" integer,
  "TXN_TYPE" integer
);
INSERT INTO "TXNS" ("TXN_ID", "TXN_STATE", "TXN_STARTED", "TXN_LAST_HEARTBEAT", "TXN_USER", "TXN_HOST")
  VALUES(0, 'c', 0, 0, '', '');

CREATE TABLE "TXN_COMPONENTS" (
  "TC_TXNID" bigint NOT NULL REFERENCES "TXNS" ("TXN_ID"),
  "TC_DATABASE" varchar(128) NOT NULL,
  "TC_TABLE" varchar(256),
  "TC_PARTITION" varchar(767) DEFAULT NULL,
  "TC_OPERATION_TYPE" char(1) NOT NULL,
  "TC_WRITEID" bigint
);

CREATE INDEX TC_TXNID_INDEX ON "TXN_COMPONENTS" USING hash ("TC_TXNID");

CREATE TABLE "COMPLETED_TXN_COMPONENTS" (
  "CTC_TXNID" bigint NOT NULL,
  "CTC_DATABASE" varchar(128) NOT NULL,
  "CTC_TABLE" varchar(256),
  "CTC_PARTITION" varchar(767),
  "CTC_TIMESTAMP" timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL,
  "CTC_WRITEID" bigint,
  "CTC_UPDATE_DELETE" char(1) NOT NULL
);

CREATE INDEX COMPLETED_TXN_COMPONENTS_INDEX ON "COMPLETED_TXN_COMPONENTS" USING btree ("CTC_DATABASE", "CTC_TABLE", "CTC_PARTITION");

CREATE TABLE "TXN_LOCK_TBL" (
  "TXN_LOCK" bigint NOT NULL
);
INSERT INTO "TXN_LOCK_TBL" VALUES(1);

CREATE TABLE "HIVE_LOCKS" (
  "HL_LOCK_EXT_ID" bigint NOT NULL,
  "HL_LOCK_INT_ID" bigint NOT NULL,
  "HL_TXNID" bigint NOT NULL,
  "HL_DB" varchar(128) NOT NULL,
  "HL_TABLE" varchar(256),
  "HL_PARTITION" varchar(767) DEFAULT NULL,
  "HL_LOCK_STATE" char(1) NOT NULL,
  "HL_LOCK_TYPE" char(1) NOT NULL,
  "HL_LAST_HEARTBEAT" bigint NOT NULL,
  "HL_ACQUIRED_AT" bigint,
  "HL_USER" varchar(128) NOT NULL,
  "HL_HOST" varchar(128) NOT NULL,
  "HL_HEARTBEAT_COUNT" integer,
  "HL_AGENT_INFO" varchar(128),
  "HL_BLOCKEDBY_EXT_ID" bigint,
  "HL_BLOCKEDBY_INT_ID" bigint,
  PRIMARY KEY("HL_LOCK_EXT_ID", "HL_LOCK_INT_ID")
);

CREATE INDEX HL_TXNID_INDEX ON "HIVE_LOCKS" USING hash ("HL_TXNID");

CREATE TABLE "NEXT_LOCK_ID" (
  "NL_NEXT" bigint NOT NULL
);
INSERT INTO "NEXT_LOCK_ID" VALUES(1);

CREATE TABLE "COMPACTION_QUEUE" (
  "CQ_ID" bigint PRIMARY KEY,
  "CQ_DATABASE" varchar(128) NOT NULL,
  "CQ_TABLE" varchar(256) NOT NULL,
  "CQ_PARTITION" varchar(767),
  "CQ_STATE" char(1) NOT NULL,
  "CQ_TYPE" char(1) NOT NULL,
  "CQ_TBLPROPERTIES" varchar(2048),
  "CQ_WORKER_ID" varchar(128),
  "CQ_ENQUEUE_TIME" bigint,
  "CQ_START" bigint,
  "CQ_RUN_AS" varchar(128),
  "CQ_HIGHEST_WRITE_ID" bigint,
  "CQ_META_INFO" bytea,
  "CQ_HADOOP_JOB_ID" varchar(32),
  "CQ_ERROR_MESSAGE" text,
  "CQ_NEXT_TXN_ID" bigint,
  "CQ_TXN_ID" bigint,
  "CQ_COMMIT_TIME" bigint,
  "CQ_INITIATOR_ID" varchar(128),
  "CQ_INITIATOR_VERSION" varchar(128),
  "CQ_WORKER_VERSION" varchar(128),
  "CQ_CLEANER_START" bigint,
  "CQ_RETRY_RETENTION" bigint not null default 0,
  "CQ_POOL_NAME" varchar(128)
);

CREATE TABLE "NEXT_COMPACTION_QUEUE_ID" (
  "NCQ_NEXT" bigint NOT NULL
);
INSERT INTO "NEXT_COMPACTION_QUEUE_ID" VALUES(1);

CREATE TABLE "COMPLETED_COMPACTIONS" (
  "CC_ID" bigint PRIMARY KEY,
  "CC_DATABASE" varchar(128) NOT NULL,
  "CC_TABLE" varchar(256) NOT NULL,
  "CC_PARTITION" varchar(767),
  "CC_STATE" char(1) NOT NULL,
  "CC_TYPE" char(1) NOT NULL,
  "CC_TBLPROPERTIES" varchar(2048),
  "CC_WORKER_ID" varchar(128),
  "CC_ENQUEUE_TIME" bigint,
  "CC_START" bigint,
  "CC_END" bigint,
  "CC_RUN_AS" varchar(128),
  "CC_HIGHEST_WRITE_ID" bigint,
  "CC_META_INFO" bytea,
  "CC_HADOOP_JOB_ID" varchar(32),
  "CC_ERROR_MESSAGE" text,
  "CC_NEXT_TXN_ID" bigint,
  "CC_TXN_ID" bigint,
  "CC_COMMIT_TIME" bigint,
  "CC_INITIATOR_ID" varchar(128),
  "CC_INITIATOR_VERSION" varchar(128),
  "CC_WORKER_VERSION" varchar(128),
  "CC_POOL_NAME" varchar(128)
);

CREATE INDEX "COMPLETED_COMPACTIONS_RES" ON "COMPLETED_COMPACTIONS" ("CC_DATABASE","CC_TABLE","CC_PARTITION");

-- HIVE-25842
CREATE TABLE "COMPACTION_METRICS_CACHE" (
    "CMC_DATABASE" varchar(128) NOT NULL,
    "CMC_TABLE" varchar(256) NOT NULL,
    "CMC_PARTITION" varchar(767),
    "CMC_METRIC_TYPE" varchar(128) NOT NULL,
    "CMC_METRIC_VALUE" integer NOT NULL,
    "CMC_VERSION" integer NOT NULL
);

CREATE TABLE "AUX_TABLE" (
  "MT_KEY1" varchar(128) NOT NULL,
  "MT_KEY2" bigint NOT NULL,
  "MT_COMMENT" varchar(255),
  PRIMARY KEY("MT_KEY1", "MT_KEY2")
);

CREATE TABLE "WRITE_SET" (
  "WS_DATABASE" varchar(128) NOT NULL,
  "WS_TABLE" varchar(256) NOT NULL,
  "WS_PARTITION" varchar(767),
  "WS_TXNID" bigint NOT NULL,
  "WS_COMMIT_ID" bigint NOT NULL,
  "WS_OPERATION_TYPE" char(1) NOT NULL
);

CREATE TABLE "TXN_TO_WRITE_ID" (
  "T2W_TXNID" bigint NOT NULL,
  "T2W_DATABASE" varchar(128) NOT NULL,
  "T2W_TABLE" varchar(256) NOT NULL,
  "T2W_WRITEID" bigint NOT NULL
);

CREATE UNIQUE INDEX "TBL_TO_TXN_ID_IDX" ON "TXN_TO_WRITE_ID" ("T2W_DATABASE", "T2W_TABLE", "T2W_TXNID");
CREATE UNIQUE INDEX "TBL_TO_WRITE_ID_IDX" ON "TXN_TO_WRITE_ID" ("T2W_DATABASE", "T2W_TABLE", "T2W_WRITEID");

CREATE TABLE "NEXT_WRITE_ID" (
  "NWI_DATABASE" varchar(128) NOT NULL,
  "NWI_TABLE" varchar(256) NOT NULL,
  "NWI_NEXT" bigint NOT NULL
);

CREATE UNIQUE INDEX "NEXT_WRITE_ID_IDX" ON "NEXT_WRITE_ID" ("NWI_DATABASE", "NWI_TABLE");

CREATE TABLE "MIN_HISTORY_LEVEL" (
  "MHL_TXNID" bigint NOT NULL,
  "MHL_MIN_OPEN_TXNID" bigint NOT NULL,
  PRIMARY KEY("MHL_TXNID")
);

CREATE INDEX "MIN_HISTORY_LEVEL_IDX" ON "MIN_HISTORY_LEVEL" ("MHL_MIN_OPEN_TXNID");

CREATE TABLE "MATERIALIZATION_REBUILD_LOCKS" (
  "MRL_TXN_ID" bigint NOT NULL,
  "MRL_DB_NAME" varchar(128) NOT NULL,
  "MRL_TBL_NAME" varchar(256) NOT NULL,
  "MRL_LAST_HEARTBEAT" bigint NOT NULL,
  PRIMARY KEY("MRL_TXN_ID")
);

CREATE TABLE "I_SCHEMA" (
  "SCHEMA_ID" bigint primary key,
  "SCHEMA_TYPE" integer not null,
  "NAME" varchar(256) unique,
  "DB_ID" bigint references "DBS" ("DB_ID"),
  "COMPATIBILITY" integer not null,
  "VALIDATION_LEVEL" integer not null,
  "CAN_EVOLVE" boolean not null,
  "SCHEMA_GROUP" varchar(256),
  "DESCRIPTION" varchar(4000)
);

CREATE TABLE "SCHEMA_VERSION" (
  "SCHEMA_VERSION_ID" bigint primary key,
  "SCHEMA_ID" bigint references "I_SCHEMA" ("SCHEMA_ID"),
  "VERSION" integer not null,
  "CREATED_AT" bigint not null,
  "CD_ID" bigint references "CDS" ("CD_ID"),
  "STATE" integer not null,
  "DESCRIPTION" varchar(4000),
  "SCHEMA_TEXT" text,
  "FINGERPRINT" varchar(256),
  "SCHEMA_VERSION_NAME" varchar(256),
  "SERDE_ID" bigint references "SERDES" ("SERDE_ID"),
  unique ("SCHEMA_ID", "VERSION")
);

CREATE TABLE "REPL_TXN_MAP" (
  "RTM_REPL_POLICY" varchar(256) NOT NULL,
  "RTM_SRC_TXN_ID" bigint NOT NULL,
  "RTM_TARGET_TXN_ID" bigint NOT NULL,
  PRIMARY KEY ("RTM_REPL_POLICY", "RTM_SRC_TXN_ID")
);


CREATE TABLE "RUNTIME_STATS" (
 "RS_ID" bigint primary key,
 "CREATE_TIME" bigint NOT NULL,
 "WEIGHT" bigint NOT NULL,
 "PAYLOAD" bytea
);

CREATE INDEX "IDX_RUNTIME_STATS_CREATE_TIME" ON "RUNTIME_STATS" ("CREATE_TIME");



CREATE TABLE "TXN_WRITE_NOTIFICATION_LOG" (
  "WNL_ID" bigint NOT NULL,
  "WNL_TXNID" bigint NOT NULL,
  "WNL_WRITEID" bigint NOT NULL,
  "WNL_DATABASE" varchar(128) NOT NULL,
  "WNL_TABLE" varchar(256) NOT NULL,
  "WNL_PARTITION" varchar(767) NOT NULL,
  "WNL_TABLE_OBJ" text NOT NULL,
  "WNL_PARTITION_OBJ" text,
  "WNL_FILES" text,
  "WNL_EVENT_TIME" integer NOT NULL,
  PRIMARY KEY ("WNL_TXNID", "WNL_DATABASE", "WNL_TABLE", "WNL_PARTITION")
);

INSERT INTO "SEQUENCE_TABLE" ("SEQUENCE_NAME", "NEXT_VAL") VALUES ('org.apache.hadoop.hive.metastore.model.MTxnWriteNotificationLog', 1);

CREATE TABLE "SCHEDULED_QUERIES" (
    "SCHEDULED_QUERY_ID" BIGINT NOT NULL,
    "CLUSTER_NAMESPACE" VARCHAR(256),
    "ENABLED" boolean NOT NULL,
    "NEXT_EXECUTION" INTEGER,
    "QUERY" VARCHAR(4000),
    "SCHEDULE" VARCHAR(256),
    "SCHEDULE_NAME" VARCHAR(256),
    "USER" VARCHAR(256),
    "ACTIVE_EXECUTION_ID" BIGINT,
    CONSTRAINT "SCHEDULED_QUERIES_PK" PRIMARY KEY ("SCHEDULED_QUERY_ID")
);

CREATE TABLE "SCHEDULED_EXECUTIONS" (
    "SCHEDULED_EXECUTION_ID" BIGINT NOT NULL,
    "END_TIME" INTEGER,
    "ERROR_MESSAGE" VARCHAR(2000),
    "EXECUTOR_QUERY_ID" VARCHAR(256),
    "LAST_UPDATE_TIME" INTEGER,
    "SCHEDULED_QUERY_ID" BIGINT,
    "START_TIME" INTEGER,
    "STATE" VARCHAR(256),
    CONSTRAINT "SCHEDULED_EXECUTIONS_PK" PRIMARY KEY ("SCHEDULED_EXECUTION_ID"),
    CONSTRAINT "SCHEDULED_EXECUTIONS_SCHQ_FK" FOREIGN KEY ("SCHEDULED_QUERY_ID") REFERENCES "SCHEDULED_QUERIES"("SCHEDULED_QUERY_ID") ON DELETE CASCADE
);

CREATE INDEX IDX_SCHEDULED_EXECUTIONS_LAST_UPDATE_TIME ON "SCHEDULED_EXECUTIONS" ("LAST_UPDATE_TIME");
CREATE INDEX IDX_SCHEDULED_EXECUTIONS_SCHEDULED_QUERY_ID ON "SCHEDULED_EXECUTIONS" ("SCHEDULED_QUERY_ID");
CREATE UNIQUE INDEX UNIQUE_SCHEDULED_EXECUTIONS_ID ON "SCHEDULED_EXECUTIONS" ("SCHEDULED_EXECUTION_ID");

--Create table replication metrics
CREATE TABLE "REPLICATION_METRICS" (
  "RM_SCHEDULED_EXECUTION_ID" bigint NOT NULL,
  "RM_POLICY" varchar(256) NOT NULL,
  "RM_DUMP_EXECUTION_ID" bigint NOT NULL,
  "RM_METADATA" varchar(4000),
  "RM_PROGRESS" varchar(10000),
  "RM_START_TIME" integer NOT NULL,
  "MESSAGE_FORMAT" VARCHAR(16) DEFAULT 'json-0.2',
  PRIMARY KEY("RM_SCHEDULED_EXECUTION_ID")
);

--Create indexes for the replication metrics table
CREATE INDEX "POLICY_IDX" ON "REPLICATION_METRICS" ("RM_POLICY");
CREATE INDEX "DUMP_IDX" ON "REPLICATION_METRICS" ("RM_DUMP_EXECUTION_ID");

-- Create stored procedure tables
  CREATE TABLE "STORED_PROCS" (
  "SP_ID" BIGINT NOT NULL,
  "CREATE_TIME" INTEGER NOT NULL,
  "DB_ID" BIGINT NOT NULL,
  "NAME" VARCHAR(256) NOT NULL,
  "OWNER_NAME" VARCHAR(128) NOT NULL,
  "SOURCE" TEXT NOT NULL,
  PRIMARY KEY ("SP_ID")
);

CREATE UNIQUE INDEX "UNIQUESTOREDPROC" ON "STORED_PROCS" ("NAME", "DB_ID");
ALTER TABLE ONLY "STORED_PROCS" ADD CONSTRAINT "STOREDPROC_FK1" FOREIGN KEY ("DB_ID") REFERENCES "DBS" ("DB_ID") DEFERRABLE;

-- Create stored procedure tables
CREATE TABLE "PACKAGES" (
  "PKG_ID" BIGINT NOT NULL,
  "CREATE_TIME" INTEGER NOT NULL,
  "DB_ID" BIGINT NOT NULL,
  "NAME" VARCHAR(256) NOT NULL,
  "OWNER_NAME" VARCHAR(128) NOT NULL,
  "HEADER" TEXT NOT NULL,
  "BODY" TEXT NOT NULL,
  PRIMARY KEY ("PKG_ID")
);

CREATE UNIQUE INDEX "UNIQUEPKG" ON "PACKAGES" ("NAME", "DB_ID");
ALTER TABLE ONLY "PACKAGES" ADD CONSTRAINT "PACKAGES_FK1" FOREIGN KEY ("DB_ID") REFERENCES "DBS" ("DB_ID")  DEFERRABLE;

-- HIVE-24396
-- Create DataConnectors and DataConnector_Params tables
CREATE TABLE "DATACONNECTORS" (
  "NAME" character varying(128) NOT NULL,
  "TYPE" character varying(32) NOT NULL,
  "URL" character varying(4000) NOT NULL,
  "COMMENT" character varying(256),
  "OWNER_NAME" character varying(256),
  "OWNER_TYPE" character varying(10),
  "CREATE_TIME" INTEGER NOT NULL,
  PRIMARY KEY ("NAME")
);

CREATE TABLE "DATACONNECTOR_PARAMS" (
  "NAME" character varying(128) NOT NULL,
  "PARAM_KEY" character varying(180) NOT NULL,
  "PARAM_VALUE" character varying(4000),
  PRIMARY KEY ("NAME", "PARAM_KEY"),
  CONSTRAINT "DATACONNECTOR_NAME_FK1" FOREIGN KEY ("NAME") REFERENCES "DATACONNECTORS"("NAME") ON DELETE CASCADE
);

ALTER TABLE ONLY "DC_PRIVS"
    ADD CONSTRAINT "DC_PRIVS_DC_ID_fkey" FOREIGN KEY ("NAME") REFERENCES "DATACONNECTORS"("NAME") DEFERRABLE;

-- -----------------------------------------------------------------
-- Record schema version. Should be the last step in the init script
-- -----------------------------------------------------------------
INSERT INTO "VERSION" ("VER_ID", "SCHEMA_VERSION", "VERSION_COMMENT") VALUES (1, '4.0.0-alpha-2', 'Hive release version 4.0.0-alpha-2');
