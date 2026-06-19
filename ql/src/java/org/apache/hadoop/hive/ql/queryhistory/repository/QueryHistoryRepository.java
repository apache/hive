/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.queryhistory.repository;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.queryhistory.schema.Record;
import org.apache.hadoop.hive.ql.queryhistory.schema.Schema;

import java.util.Queue;

public interface QueryHistoryRepository {
  String QUERY_HISTORY_DB_NAME = "sys";
  String QUERY_HISTORY_TABLE_NAME = "query_history";
  String QUERY_HISTORY_DB_TABLE_NAME =
      String.format("%s.%s", QUERY_HISTORY_DB_NAME, QUERY_HISTORY_TABLE_NAME);

  String QUERY_HISTORY_DB_COMMENT = "Hive SYS database";

  // an artificial hive query id for an artificial session for creating the iceberg query history table
  // this is needed because the partition_transform_spec is handed over through SessionState resource which
  // needs a QueryState instance (acquired by a query id)
  String QUERY_ID_FOR_TABLE_CREATION = "query_history_service_query_id_for_table_creation";

  void init(HiveConf conf, Schema schema);

  void flush(Queue<Record> records);
}
