SELECT '< HIVE-3764 Support metastore version consistency check >';

--
-- Table structure for VERSION
--
CREATE TABLE "VERSION" (
  "VER_ID" bigint,
  "SCHEMA_VERSION" character varying(127) NOT NULL,
  "VERSION_COMMENT" character varying(255) NOT NULL
);
ALTER TABLE ONLY "VERSION" ADD CONSTRAINT "VERSION_pkey" PRIMARY KEY ("VER_ID");

INSERT INTO "VERSION" ("VER_ID", "SCHEMA_VERSION", "VERSION_COMMENT") VALUES (1, '', 'Initial value');
