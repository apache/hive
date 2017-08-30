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
  public static final String NUM_TIMED_OUT_TXNS = "num_timed_out_transactions";

  public static final String OPEN_CONNECTIONS = "open_connections";

  public static final String TOTAL_DATABASES = "total_count_dbs";
  public static final String TOTAL_TABLES = "total_count_tables";
  public static final String TOTAL_PARTITIONS = "total_count_partitions";
}
