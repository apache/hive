SELECT '< Upgrading MetaStore schema from 0.5.0 to 0.6.0 >';
-- HIVE-972: Support views
ALTER TABLE "TBLS" ADD COLUMN "VIEW_EXPANDED_TEXT" text;
ALTER TABLE "TBLS" ADD COLUMN "VIEW_ORIGINAL_TEXT" text;

-- HIVE-1068: CREATE VIEW followup: add a 'table type' enum
--            attribute in metastore
ALTER TABLE "TBLS" ADD COLUMN "TBL_TYPE" character varying(128);

-- HIVE-675: Add database/schema support for Hive QL
ALTER TABLE "DBS" ALTER "DESC" TYPE character varying(4000);
ALTER TABLE "DBS" ADD COLUMN "DB_LOCATION_URI" character varying(4000) NOT NULL DEFAULT ''::character varying;

-- HIVE-1364: Increase the maximum length of various metastore fields,
--            and remove TYPE_NAME from COLUMNS primary key
ALTER TABLE "TBLS" ALTER "OWNER" TYPE character varying(767);
ALTER TABLE "COLUMNS" ALTER "TYPE_NAME" TYPE character varying(4000);
ALTER TABLE "PARTITION_KEYS" ALTER "PKEY_COMMENT" TYPE character varying(4000);
ALTER TABLE "SD_PARAMS" ALTER "PARAM_VALUE" TYPE character varying(4000);
ALTER TABLE "SDS" ALTER "INPUT_FORMAT" TYPE character varying(4000);
ALTER TABLE "SDS" ALTER "LOCATION" TYPE character varying(4000);
ALTER TABLE "SDS" ALTER "OUTPUT_FORMAT" TYPE character varying(4000);
ALTER TABLE "SERDE_PARAMS" ALTER "PARAM_VALUE" TYPE character varying(4000);
ALTER TABLE "SERDES" ALTER "SLIB" TYPE character varying(4000);
ALTER TABLE "TABLE_PARAMS" ALTER "PARAM_VALUE" TYPE character varying(4000);
ALTER TABLE "COLUMNS" DROP CONSTRAINT "COLUMNS_pkey";
ALTER TABLE "COLUMNS" ADD CONSTRAINT "COLUMNS_pkey" PRIMARY KEY ("SD_ID", "COLUMN_NAME");
ALTER TABLE "PARTITION_PARAMS" ALTER "PARAM_VALUE" TYPE character varying(4000);

SELECT '< Finished upgrading MetaStore schema from 0.5.0 to 0.6.0 >';
