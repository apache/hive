-- Licensed to the Apache Software Foundation (ASF) under one or more
-- contributor license agreements.  See the NOTICE file distributed with
-- this work for additional information regarding copyright ownership.
-- The ASF licenses this file to You under the Apache License, Version 2.0
-- (the License); you may not use this file except in compliance with
-- the License.  You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an AS IS BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

--
-- Tables for transaction management
--

CREATE TABLE COMPACTION_QUEUE(
	CQ_ID bigint NOT NULL,
	CQ_DATABASE varchar(128) NOT NULL,
	CQ_TABLE varchar(128) NOT NULL,
	CQ_PARTITION varchar(767) NULL,
	CQ_STATE char(1) NOT NULL,
	CQ_TYPE char(1) NOT NULL,
	CQ_WORKER_ID varchar(128) NULL,
	CQ_START bigint NULL,
	CQ_RUN_AS varchar(128) NULL,
PRIMARY KEY CLUSTERED 
(
	CQ_ID ASC
)
);

CREATE TABLE COMPLETED_TXN_COMPONENTS(
	CTC_TXNID bigint NULL,
	CTC_DATABASE varchar(128) NOT NULL,
	CTC_TABLE varchar(128) NULL,
	CTC_PARTITION varchar(767) NULL
);

CREATE TABLE HIVE_LOCKS(
	HL_LOCK_EXT_ID bigint NOT NULL,
	HL_LOCK_INT_ID bigint NOT NULL,
	HL_TXNID bigint NULL,
	HL_DB varchar(128) NOT NULL,
	HL_TABLE varchar(128) NULL,
	HL_PARTITION varchar(767) NULL,
	HL_LOCK_STATE char(1) NOT NULL,
	HL_LOCK_TYPE char(1) NOT NULL,
	HL_LAST_HEARTBEAT bigint NOT NULL,
	HL_ACQUIRED_AT bigint NULL,
	HL_USER varchar(128) NOT NULL,
	HL_HOST varchar(128) NOT NULL,
PRIMARY KEY CLUSTERED 
(
	HL_LOCK_EXT_ID ASC,
	HL_LOCK_INT_ID ASC
)
);

CREATE TABLE NEXT_COMPACTION_QUEUE_ID(
	NCQ_NEXT bigint NOT NULL
);

INSERT INTO NEXT_COMPACTION_QUEUE_ID VALUES(1);

CREATE TABLE NEXT_LOCK_ID(
	NL_NEXT bigint NOT NULL
);

INSERT INTO NEXT_LOCK_ID VALUES(1);

CREATE TABLE NEXT_TXN_ID(
	NTXN_NEXT bigint NOT NULL
);

INSERT INTO NEXT_TXN_ID VALUES(1);

CREATE TABLE TXNS(
	TXN_ID bigint NOT NULL,
	TXN_STATE char(1) NOT NULL,
	TXN_STARTED bigint NOT NULL,
	TXN_LAST_HEARTBEAT bigint NOT NULL,
	TXN_USER varchar(128) NOT NULL,
	TXN_HOST varchar(128) NOT NULL,
PRIMARY KEY CLUSTERED 
(
	TXN_ID ASC
)
);

CREATE TABLE TXN_COMPONENTS(
	TC_TXNID bigint NULL,
	TC_DATABASE varchar(128) NOT NULL,
	TC_TABLE varchar(128) NULL,
	TC_PARTITION varchar(767) NULL
);

ALTER TABLE TXN_COMPONENTS  WITH CHECK ADD FOREIGN KEY(TC_TXNID) REFERENCES TXNS (TXN_ID);
