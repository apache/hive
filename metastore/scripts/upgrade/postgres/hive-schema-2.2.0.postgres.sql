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


--
-- Name: DBS; Type: TABLE; Schema: public; Owner: hiveuser; Tablespace:
--

CREATE TABLE "DBS" (
    "DB_ID" bigint NOT NULL,
    "DESC" character varying(4000) DEFAULT NULL::character varying,
    "DB_LOCATION_URI" character varying(4000) NOT NULL,
    "NAME" character varying(128) DEFAULT NULL::character varying,
    "OWNER_NAME" character varying(128) DEFAULT NULL::character varying,
    "OWNER_TYPE" character varying(10) DEFAULT NULL::character varying
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
    "DB_PRIV" character varying(128) DEFAULT NULL::character varying
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
    "USER_PRIV" character varying(128) DEFAULT NULL::character varying
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
    "TBL_ID" bigint
);


--
-- Name: PARTITION_EVENTS; Type: TABLE; Schema: public; Owner: hiveuser; Tablespace:
--

CREATE TABLE "PARTITION_EVENTS" (
    "PART_NAME_ID" bigint NOT NULL,
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
    "PARAM_VALUE" character varying(4000) DEFAULT NULL::character varying
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
    "PART_COL_PRIV" character varying(128) DEFAULT NULL::character varying
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
    "PART_PRIV" character varying(128) DEFAULT NULL::character varying
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


--
-- Name: SERDES; Type: TABLE; Schema: public; Owner: hiveuser; Tablespace:
--

CREATE TABLE "SERDES" (
    "SERDE_ID" bigint NOT NULL,
    "NAME" character varying(128) DEFAULT NULL::character varying,
    "SLIB" character varying(4000) DEFAULT NULL::character varying
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
    "RETENTION" bigint NOT NULL,
    "SD_ID" bigint,
    "TBL_NAME" character varying(256) DEFAULT NULL::character varying,
    "TBL_TYPE" character varying(128) DEFAULT NULL::character varying,
    "VIEW_EXPANDED_TEXT" text,
    "VIEW_ORIGINAL_TEXT" text,
    "IS_REWRITE_ENABLED" boolean NOT NULL
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
    "TBL_ID" bigint
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
    "TBL_ID" bigint
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
 "AVG_COL_LEN" double precision,
 "MAX_COL_LEN" bigint,
 "NUM_TRUES" bigint,
 "NUM_FALSES" bigint,
 "LAST_ANALYZED" bigint NOT NULL
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
 "AVG_COL_LEN" double precision,
 "MAX_COL_LEN" bigint,
 "NUM_TRUES" bigint,
 "NUM_FALSES" bigint,
 "LAST_ANALYZED" bigint NOT NULL
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
    "DB_NAME" VARCHAR(128),
    "TBL_NAME" VARCHAR(256),
    "MESSAGE" text,
    "MESSAGE_FORMAT" VARCHAR(16),
    PRIMARY KEY ("NL_ID")
);

CREATE TABLE "NOTIFICATION_SEQUENCE"
(
    "NNI_ID" BIGINT NOT NULL,
    "NEXT_EVENT_ID" BIGINT NOT NULL,
    PRIMARY KEY ("NNI_ID")
);

CREATE TABLE "KEY_CONSTRAINTS"
(
  "CHILD_CD_ID" BIGINT,
  "CHILD_INTEGER_IDX" BIGINT,
  "CHILD_TBL_ID" BIGINT,
  "PARENT_CD_ID" BIGINT NOT NULL,
  "PARENT_INTEGER_IDX" BIGINT NOT NULL,
  "PARENT_TBL_ID" BIGINT NOT NULL,
  "POSITION" BIGINT NOT NULL,
  "CONSTRAINT_NAME" VARCHAR(400) NOT NULL,
  "CONSTRAINT_TYPE" SMALLINT NOT NULL,
  "UPDATE_RULE" SMALLINT,
  "DELETE_RULE"	SMALLINT,
  "ENABLE_VALIDATE_RELY" SMALLINT NOT NULL,
  PRIMARY KEY ("CONSTRAINT_NAME", "POSITION")
) ;

CREATE INDEX "CONSTRAINTS_PARENT_TBLID_INDEX" ON "KEY_CONSTRAINTS" USING BTREE ("PARENT_TBL_ID");

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
    ADD CONSTRAINT "DBPRIVILEGEINDEX" UNIQUE ("DB_ID", "PRINCIPAL_NAME", "PRINCIPAL_TYPE", "DB_PRIV", "GRANTOR", "GRANTOR_TYPE");


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
-- Name: GLOBALPRIVILEGEINDEX; Type: CONSTRAINT; Schema: public; Owner: hiveuser; Tablespace:
--

ALTER TABLE ONLY "GLOBAL_PRIVS"
    ADD CONSTRAINT "GLOBALPRIVILEGEINDEX" UNIQUE ("PRINCIPAL_NAME", "PRINCIPAL_TYPE", "USER_PRIV", "GRANTOR", "GRANTOR_TYPE");


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
    ADD CONSTRAINT "UNIQUE_DATABASE" UNIQUE ("NAME");


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

CREATE INDEX "PARTITIONCOLUMNPRIVILEGEINDEX" ON "PART_COL_PRIVS" USING btree ("PART_ID", "COLUMN_NAME", "PRINCIPAL_NAME", "PRINCIPAL_TYPE", "PART_COL_PRIV", "GRANTOR", "GRANTOR_TYPE");


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

CREATE INDEX "PARTPRIVILEGEINDEX" ON "PART_PRIVS" USING btree ("PART_ID", "PRINCIPAL_NAME", "PRINCIPAL_TYPE", "PART_PRIV", "GRANTOR", "GRANTOR_TYPE");


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

CREATE INDEX "PCS_STATS_IDX" ON "PART_COL_STATS" USING btree ("DB_NAME","TABLE_NAME","COLUMN_NAME","PARTITION_NAME");


--
-- Name: ROLE_MAP_N49; Type: INDEX; Schema: public; Owner: hiveuser; Tablespace:
--

CREATE INDEX "ROLE_MAP_N49" ON "ROLE_MAP" USING btree ("ROLE_ID");


--
-- Name: SDS_N49; Type: INDEX; Schema: public; Owner: hiveuser; Tablespace:
--

CREATE INDEX "SDS_N49" ON "SDS" USING btree ("SERDE_ID");


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

CREATE INDEX "TABLECOLUMNPRIVILEGEINDEX" ON "TBL_COL_PRIVS" USING btree ("TBL_ID", "COLUMN_NAME", "PRINCIPAL_NAME", "PRINCIPAL_TYPE", "TBL_COL_PRIV", "GRANTOR", "GRANTOR_TYPE");


--
-- Name: TABLEPRIVILEGEINDEX; Type: INDEX; Schema: public; Owner: hiveuser; Tablespace:
--

CREATE INDEX "TABLEPRIVILEGEINDEX" ON "TBL_PRIVS" USING btree ("TBL_ID", "PRINCIPAL_NAME", "PRINCIPAL_TYPE", "TBL_PRIV", "GRANTOR", "GRANTOR_TYPE");


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


ALTER TABLE ONLY "VERSION" ADD CONSTRAINT "VERSION_pkey" PRIMARY KEY ("VER_ID");

-- Name: FUNCS_FK1; Type: FK CONSTRAINT; Schema: public; Owner: hiveuser
ALTER TABLE ONLY "FUNCS"
    ADD CONSTRAINT "FUNCS_FK1" FOREIGN KEY ("DB_ID") REFERENCES "DBS" ("DB_ID") DEFERRABLE;

-- Name: FUNC_RU_FK1; Type: FK CONSTRAINT; Schema: public; Owner: hiveuser
ALTER TABLE ONLY "FUNC_RU"
    ADD CONSTRAINT "FUNC_RU_FK1" FOREIGN KEY ("FUNC_ID") REFERENCES "FUNCS" ("FUNC_ID") DEFERRABLE;

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
\i hive-txn-schema-2.2.0.postgres.sql;

-- -----------------------------------------------------------------
-- Record schema version. Should be the last step in the init script
-- -----------------------------------------------------------------
INSERT INTO "VERSION" ("VER_ID", "SCHEMA_VERSION", "VERSION_COMMENT") VALUES (1, '2.2.0', 'Hive release version 2.2.0');
