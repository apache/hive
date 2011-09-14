SELECT '< HIVE-675: Add database/schema support for Hive QL >';
ALTER TABLE "DBS" ALTER "DESC" TYPE character varying(4000);
ALTER TABLE "DBS" ADD COLUMN "DB_LOCATION_URI" character varying(4000) NOT NULL DEFAULT ''::character varying;

