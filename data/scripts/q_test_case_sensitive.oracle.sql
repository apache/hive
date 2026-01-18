-- The comments below are from q_test_country_table_with_schema.oracle.sql

-- In Oracle dividing the tables in different namespaces/schemas is achieved via different users. The CREATE SCHEMA
-- statement exists in Oracle but has different semantics than those defined by SQL Standard and those adopted in other
-- DBMS.

-- In order to create the so-called "local" users in oracle you need to be connected to the Pluggable Database (PDB)
-- and not to the Container Database (CDB). In Oracle XE edition, used by this tests, the default and only PDB is
-- XEPDB1.
ALTER SESSION SET CONTAINER = XEPDB1;

-- Create a case-sensitive user, which also acts as the schema
CREATE USER "WorldData" IDENTIFIED BY QTestPassword123;
ALTER USER "WorldData" QUOTA UNLIMITED ON users;
GRANT CREATE SESSION, CREATE TABLE, CREATE VIEW TO "WorldData";

-- Case-Sensitive Schema and Table
CREATE TABLE "WorldData"."Country"
(
    id   int,
    name varchar(20)
);
INSERT INTO "WorldData"."Country" VALUES (1, 'India');
INSERT INTO "WorldData"."Country" VALUES (2, 'USA');
INSERT INTO "WorldData"."Country" VALUES (3, 'Japan');
INSERT INTO "WorldData"."Country" VALUES (4, 'Germany');


-- Case-Sensitive Partition Column
CREATE TABLE "WorldData"."Cities"
(
    id   int,
    name varchar(20),
    "RegionID" int
);
INSERT INTO "WorldData"."Cities" VALUES (1, 'Mumbai', 10);
INSERT INTO "WorldData"."Cities" VALUES (2, 'New York', 20);
INSERT INTO "WorldData"."Cities" VALUES (3, 'Tokyo', 30);
INSERT INTO "WorldData"."Cities" VALUES (4, 'Berlin', 40);
INSERT INTO "WorldData"."Cities" VALUES (5, 'New Delhi', 10);
INSERT INTO "WorldData"."Cities" VALUES (6, 'Kyoto', 30);


-- Case-Sensitive Query Field Names
CREATE TABLE "WorldData"."Geography"
(
    id int,
    "Description" varchar(50)
);
INSERT INTO "WorldData"."Geography" VALUES (1, 'Asia');
INSERT INTO "WorldData"."Geography" VALUES (2, 'North America');
INSERT INTO "WorldData"."Geography" VALUES (3, 'Asia');
INSERT INTO "WorldData"."Geography" VALUES (4, 'Europe');
