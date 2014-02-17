SELECT '< HIVE-6386 Database should have an owner >';

ALTER TABLE "DBS" ADD COLUMN "OWNER_NAME" character varying(128);
ALTER TABLE "DBS" ADD COLUMN "OWNER_TYPE" character varying(10);
