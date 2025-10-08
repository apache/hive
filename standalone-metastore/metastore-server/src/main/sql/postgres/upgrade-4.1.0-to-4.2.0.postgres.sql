SELECT 'Upgrading MetaStore schema from 4.1.0 to 4.2.0';

-- HIVE-29178
CREATE TABLE "CATALOG_PARAMS" (
    "CTLG_ID" BIGINT NOT NULL,
    "PARAM_KEY" VARCHAR(180) NOT NULL,
    "PARAM_VALUE" VARCHAR(4000) DEFAULT NULL,
    PRIMARY KEY ("CTLG_ID", "PARAM_KEY"),
    CONSTRAINT "CATALOG_PARAMS_FK1" FOREIGN KEY ("CTLG_ID") REFERENCES "CTLGS" ("CTLG_ID") ON DELETE CASCADE
)

-- These lines need to be last. Insert any changes above.
UPDATE "VERSION" SET "SCHEMA_VERSION"='4.2.0', "VERSION_COMMENT"='Hive release version 4.2.0' where "VER_ID"=1;
SELECT 'Finished upgrading MetaStore schema from 4.1.0 to 4.2.0';
