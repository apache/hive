-- Upgrades for Schema Registry objects
ALTER TABLE "APP"."SERDES" ADD COLUMN "DESCRIPTION" VARCHAR(4000);
ALTER TABLE "APP"."SERDES" ADD COLUMN "SERIALIZER_CLASS" VARCHAR(4000);
ALTER TABLE "APP"."SERDES" ADD COLUMN "DESERIALIZER_CLASS" VARCHAR(4000);
ALTER TABLE "APP"."SERDES" ADD COLUMN "SERDE_TYPE" INTEGER;

CREATE TABLE "APP"."I_SCHEMA" (
  "SCHEMA_ID" bigint primary key,
  "SCHEMA_TYPE" integer not null,
  "NAME" varchar(256) unique,
  "DB_ID" bigint references "APP"."DBS" ("DB_ID"),
  "COMPATIBILITY" integer not null,
  "VALIDATION_LEVEL" integer not null,
  "CAN_EVOLVE" char(1) not null,
  "SCHEMA_GROUP" varchar(256),
  "DESCRIPTION" varchar(4000)
);

CREATE TABLE "APP"."SCHEMA_VERSION" (
  "SCHEMA_VERSION_ID" bigint primary key,
  "SCHEMA_ID" bigint references "APP"."I_SCHEMA" ("SCHEMA_ID"),
  "VERSION" integer not null,
  "CREATED_AT" bigint not null,
  "CD_ID" bigint references "APP"."CDS" ("CD_ID"),
  "STATE" integer not null,
  "DESCRIPTION" varchar(4000),
  "SCHEMA_TEXT" clob,
  "FINGERPRINT" varchar(256),
  "SCHEMA_VERSION_NAME" varchar(256),
  "SERDE_ID" bigint references "APP"."SERDES" ("SERDE_ID")
);

CREATE UNIQUE INDEX "APP"."UNIQUE_SCHEMA_VERSION" ON "APP"."SCHEMA_VERSION" ("SCHEMA_ID", "VERSION");
