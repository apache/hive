-- Licensed to the Apache Software Foundation (ASF) under one or more
-- contributor license agreements.  See the NOTICE file distributed with
-- this work for additional information regarding copyright ownership.
-- The ASF licenses this file to You under the Apache License, Version 2.0
-- (the "License"); you may not use this file except in compliance with
-- the License.  You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

--
-- Tables for transaction management
-- 

CREATE TABLE "txns" (
  "txn_id" bigint PRIMARY KEY,
  "txn_state" char(1) NOT NULL,
  "txn_started" bigint NOT NULL,
  "txn_last_heartbeat" bigint NOT NULL,
  "txn_user" varchar(128) NOT NULL,
  "txn_host" varchar(128) NOT NULL
);

CREATE TABLE "txn_components" (
  "tc_txnid" bigint REFERENCES "txns" ("txn_id"),
  "tc_database" varchar(128) NOT NULL,
  "tc_table" varchar(128),
  "tc_partition" varchar(767) DEFAULT NULL
);

CREATE TABLE "completed_txn_components" (
  "ctc_txnid" bigint,
  "ctc_database" varchar(128) NOT NULL,
  "ctc_table" varchar(128),
  "ctc_partition" varchar(767)
);

CREATE TABLE "next_txn_id" (
  "ntxn_next" bigint NOT NULL
);
INSERT INTO "next_txn_id" VALUES(1);

CREATE TABLE "hive_locks" (
  "hl_lock_ext_id" bigint NOT NULL,
  "hl_lock_int_id" bigint NOT NULL,
  "hl_txnid" bigint,
  "hl_db" varchar(128) NOT NULL,
  "hl_table" varchar(128),
  "hl_partition" varchar(767) DEFAULT NULL,
  "hl_lock_state" char(1) NOT NULL,
  "hl_lock_type" char(1) NOT NULL,
  "hl_last_heartbeat" bigint NOT NULL,
  "hl_acquired_at" bigint,
  "hl_user" varchar(128) NOT NULL,
  "hl_host" varchar(128) NOT NULL,
  PRIMARY KEY("hl_lock_ext_id", "hl_lock_int_id")
); 

CREATE INDEX "hl_txnid_index" ON "hive_locks" USING hash ("hl_txnid");

CREATE TABLE "next_lock_id" (
  "nl_next" bigint NOT NULL
);
INSERT INTO "next_lock_id" VALUES(1);

CREATE TABLE "compaction_queue" (
  "cq_id" bigint PRIMARY KEY,
  "cq_database" varchar(128) NOT NULL,
  "cq_table" varchar(128) NOT NULL,
  "cq_partition" varchar(767),
  "cq_state" char(1) NOT NULL,
  "cq_type" char(1) NOT NULL,
  "cq_worker_id" varchar(128),
  "cq_start" bigint,
  "cq_run_as" varchar(128)
);

CREATE TABLE "next_compaction_queue_id" (
  "ncq_next" bigint NOT NULL
);
INSERT INTO "next_compaction_queue_id" VALUES(1);

