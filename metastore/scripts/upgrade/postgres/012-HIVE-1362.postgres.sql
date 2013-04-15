SELECT '< HIVE-1362 Column Statistics Support in Hive  >';

CREATE TABLE "TAB_COL_STATS" (
 "CS_ID" bigint NOT NULL,
 "DB_NAME" character varying(128) DEFAULT NULL::character varying,
 "TABLE_NAME" character varying(128) DEFAULT NULL::character varying,
 "COLUMN_NAME" character varying(128) DEFAULT NULL::character varying,
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


ALTER TABLE ONLY "TAB_COL_STATS" ADD CONSTRAINT "TAB_COL_STATS_pkey" PRIMARY KEY("CS_ID");
ALTER TABLE ONLY "TAB_COL_STATS" ADD CONSTRAINT "TAB_COL_STATS_fkey" FOREIGN KEY("TBL_ID") REFERENCES "TBLS"("TBL_ID") DEFERRABLE;
CREATE INDEX "TAB_COL_STATS_N49" ON "TAB_COL_STATS" USING btree ("TBL_ID");

CREATE TABLE "PART_COL_STATS" (
 "CS_ID" bigint NOT NULL,
 "DB_NAME" character varying(128) DEFAULT NULL::character varying,
 "TABLE_NAME" character varying(128) DEFAULT NULL::character varying,
 "PARTITION_NAME" character varying(767) DEFAULT NULL::character varying,
 "COLUMN_NAME" character varying(128) DEFAULT NULL::character varying,
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

ALTER TABLE ONLY "PART_COL_STATS" ADD CONSTRAINT "PART_COL_STATS_pkey" PRIMARY KEY("CS_ID");
ALTER TABLE ONLY "PART_COL_STATS" ADD CONSTRAINT "PART_COL_STATS_fkey" FOREIGN KEY("PART_ID") REFERENCES "PARTITIONS"("PART_ID") DEFERRABLE;
CREATE INDEX "PART_COL_STATS_N49" ON "PART_COL_STATS" USING btree ("PART_ID");
