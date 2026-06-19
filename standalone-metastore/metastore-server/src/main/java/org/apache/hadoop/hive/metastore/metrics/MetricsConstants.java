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
package org.apache.hadoop.hive.metastore.metrics;

public class MetricsConstants {
  public static final String ACTIVE_CALLS = "active_calls_";
  public static final String API_PREFIX = "api_";
  public static final String COMPACTION_STATUS_PREFIX = "compaction_num_";
  public static final String COMPACTION_OLDEST_ENQUEUE_AGE = "compaction_oldest_enqueue_age_in_sec";
  public static final String COMPACTION_OLDEST_WORKING_AGE = "compaction_oldest_working_age_in_sec";
  public static final String COMPACTION_OLDEST_CLEANING_AGE = "compaction_oldest_cleaning_age_in_sec";
  public static final String COMPACTION_INITIATOR_CYCLE = "compaction_initiator_cycle";
  public static final String COMPACTION_INITIATOR_CYCLE_DURATION = "compaction_initiator_cycle_duration";
  public static final String COMPACTION_INITIATOR_FAILURE_COUNTER = "compaction_initiator_failure_counter";
  public static final String COMPACTION_CLEANER_CYCLE = "compaction_cleaner_cycle";
  public static final String COMPACTION_CLEANER_CYCLE_DURATION = "compaction_cleaner_cycle_duration";
  public static final String COMPACTION_CLEANER_FAILURE_COUNTER = "compaction_cleaner_failure_counter";
  public static final String COMPACTION_WORKER_CYCLE = "compaction_worker_cycle";
  public static final String COMPACTION_POOLS_INITIATED_ITEM_COUNT = "compaction_pools_initiated_item_count";
  public static final String COMPACTION_POOLS_WORKING_ITEM_COUNT = "compaction_pools_working_item_count";
  public static final String COMPACTION_POOLS_OLDEST_INITIATED_AGE = "compaction_pools_oldest_enqueue_age_in_sec";
  public static final String COMPACTION_POOLS_OLDEST_WORKING_AGE = "compaction_pools_oldest_working_age_in_sec";

  public static final String OLDEST_OPEN_REPL_TXN_ID = "oldest_open_repl_txn_id";
  public static final String OLDEST_OPEN_NON_REPL_TXN_ID = "oldest_open_non_repl_txn_id";
  public static final String OLDEST_OPEN_REPL_TXN_AGE = "oldest_open_repl_txn_age_in_sec";
  public static final String OLDEST_OPEN_NON_REPL_TXN_AGE = "oldest_open_non_repl_txn_age_in_sec";
  // number of aborted txns in TXNS table
  public static final String NUM_ABORTED_TXNS = "num_aborted_transactions";
  public static final String OLDEST_ABORTED_TXN_ID = "oldest_aborted_txn_id";
  public static final String OLDEST_ABORTED_TXN_AGE = "oldest_aborted_txn_age_in_sec";

  public static final String COMPACTION_NUM_OBSOLETE_DELTAS = COMPACTION_STATUS_PREFIX + "obsolete_deltas";
  public static final String COMPACTION_NUM_DELTAS = COMPACTION_STATUS_PREFIX + "active_deltas";
  public static final String COMPACTION_NUM_SMALL_DELTAS = COMPACTION_STATUS_PREFIX + "small_deltas";

  public static final String NUM_LOCKS = "num_locks";
  public static final String OLDEST_LOCK_AGE = "oldest_lock_age_in_sec";

  public static final String NUM_TXN_TO_WRITEID = MetricsConstants.COMPACTION_STATUS_PREFIX + "txn_to_writeid";
  public static final String NUM_COMPLETED_TXN_COMPONENTS = MetricsConstants.COMPACTION_STATUS_PREFIX + "completed_txn_components";

  public static final String COMPACTION_NUM_INITIATORS = COMPACTION_STATUS_PREFIX + "initiators";
  public static final String COMPACTION_NUM_WORKERS = COMPACTION_STATUS_PREFIX + "workers";
  public static final String COMPACTION_NUM_INITIATOR_VERSIONS = COMPACTION_STATUS_PREFIX + "initiator_versions";
  public static final String COMPACTION_NUM_WORKER_VERSIONS = COMPACTION_STATUS_PREFIX + "worker_versions";

  public static final String TOTAL_API_CALLS = "total_api_calls";

  public static final String CREATE_TOTAL_DATABASES = "create_total_count_dbs";
  public static final String CREATE_TOTAL_TABLES = "create_total_count_tables";
  public static final String CREATE_TOTAL_PARTITIONS = "create_total_count_partitions";

  public static final String DELETE_TOTAL_DATABASES = "delete_total_count_dbs";
  public static final String DELETE_TOTAL_TABLES = "delete_total_count_tables";
  public static final String DELETE_TOTAL_PARTITIONS = "delete_total_count_partitions";

  public static final String DIRECTSQL_ERRORS = "directsql_errors";

  public static final String JVM_PAUSE_INFO = "jvm.pause.info-threshold";
  public static final String JVM_PAUSE_WARN = "jvm.pause.warn-threshold";
  public static final String JVM_EXTRA_SLEEP = "jvm.pause.extraSleepTime";

  public static final String NUM_OPEN_TXNS = "num_open_transactions";
  public static final String NUM_OPEN_REPL_TXNS = "num_open_repl_transactions";
  public static final String NUM_OPEN_NON_REPL_TXNS = "num_open_non_repl_transactions";
  public static final String TOTAL_NUM_ABORTED_TXNS = "total_num_aborted_transactions";
  public static final String TOTAL_NUM_COMMITTED_TXNS = "total_num_committed_transactions";
  public static final String TOTAL_NUM_TIMED_OUT_TXNS = "total_num_timed_out_transactions";

  public static final String OPEN_CONNECTIONS = "open_connections";

  public static final String TOTAL_DATABASES = "total_count_dbs";
  public static final String TOTAL_TABLES = "total_count_tables";
  public static final String TOTAL_PARTITIONS = "total_count_partitions";

  public static final String TABLES_WITH_X_ABORTED_TXNS = "tables_with_x_aborted_transactions";

  public static final String WRITES_TO_DISABLED_COMPACTION_TABLE = "num_writes_to_disabled_compaction_table";

  public static final String OLDEST_READY_FOR_CLEANING_AGE = "oldest_ready_for_cleaning_age_in_sec";
}
