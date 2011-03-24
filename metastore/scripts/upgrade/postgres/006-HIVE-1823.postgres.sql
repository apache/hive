SELECT '< HIVE-1823 Upgrade the database thrift interface to allow parameters key-value pairs >';

--
-- Table: DATABASE_PARAMS
--

CREATE TABLE "DATABASE_PARAMS" (
  "DB_ID" bigint NOT NULL,
  "PARAM_KEY" character varying(180) NOT NULL,
  "PARAM_VALUE" character varying(4000) DEFAULT NULL,
  PRIMARY KEY ("DB_ID", "PARAM_KEY")
);

--
-- Foreign Key Definitions
--

ALTER TABLE "DATABASE_PARAMS" ADD FOREIGN KEY ("DB_ID")
  REFERENCES "DBS" ("DB_ID") DEFERRABLE;

