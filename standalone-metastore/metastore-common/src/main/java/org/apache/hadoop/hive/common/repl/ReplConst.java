/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.common.repl;

/**
 * A class that defines the constant strings used by the replication implementation.
 */

public class ReplConst {

  /**
   * The constant that denotes the table data location is changed to different path. This indicates
   * Metastore to update corresponding path in Partitions and also need to delete old path.
   */
  public static final String REPL_DATA_LOCATION_CHANGED = "REPL_DATA_LOCATION_CHANGED";

  public static final String TRUE = "true";

  /**
   * The constant string literal added as a property of database being replicated into. We choose
   * this property over other properties is because this property is added right when the
   * database is created as part of repl load and survives the incremental cycles.
   */
  public static final String REPL_TARGET_DB_PROPERTY = "hive.repl.ckpt.key";

  /**
   * A table which is target of replication will have this property set. The property serves two
   * purposes, 1. identifies the tables being replicated into and 2. records the event id of the
   * last event affecting this table.
   */
  public static final String REPL_TARGET_TABLE_PROPERTY = "repl.last.id";

  /**
   * Database level prop to identify the failover endPoint of the database.
   * It is set during planned failover and unset or removed after optimised
   * bootstrap is completed. During unplanned failover this prop is not set
   * */
  public static final String REPL_FAILOVER_ENDPOINT = "repl.failover.endpoint";

  public static final String TARGET_OF_REPLICATION = "repl.target.for";

  public static final String REPL_INCOMPATIBLE = "repl.incompatible";

  /**
   * Database level prop to enable background threads for the database irrespective of other replication props.
   * */
  public static final String REPL_ENABLE_BACKGROUND_THREAD = "repl.background.enable";

  /**
   * Tracks the event id with respect to the target cluster.
   */
  public static final String REPL_TARGET_DATABASE_PROPERTY = "repl.target.last.id";

  /**
   * Indicates initiation of RESUME action. This property can be used to avoid updation of
   * "repl.target.last.id" when "repl.last.id" is changed(This behaviour is currently used in
   * RESUME workflow). This property will be removed after second cycle of Optimised bootstrap.
   */
  public static final String REPL_RESUME_STARTED_AFTER_FAILOVER = "repl.resume.started";

  public static final String SOURCE_OF_REPLICATION = "repl.source.for";

  public static final String REPL_FIRST_INC_PENDING_FLAG = "hive.repl.first.inc.pending";

  public static final String REPL_IS_CUSTOM_DB_LOC = "hive.repl.is.custom.db.loc";

  public static final String REPL_IS_CUSTOM_DB_MANAGEDLOC = "hive.repl.is.custom.db.managedloc";

  public static final String BOOTSTRAP_DUMP_STATE_KEY_PREFIX = "bootstrap.dump.state.";

  public static final String READ_ONLY_HOOK = "org.apache.hadoop.hive.ql.hooks.EnforceReadOnlyDatabaseHook";

  /**
   * Type of failover
   */
  public enum FailoverType {
    PLANNED,
    UNPLANNED;
  }

  /**
   * Database level property to keep track of failover and failback metrics
   * Property values are set during failover and optimised bootstrap time.
   */
  public static final String REPL_METRICS_FAILOVER_COUNT = "repl.metrics.failover.count";
  public static final String REPL_METRICS_LAST_FAILOVER_TYPE = "repl.metrics.last.failover.type";
  public static final String REPL_METRICS_FAILBACK_COUNT = "repl.metrics.failback.count";
  public static final String REPL_METRICS_LAST_FAILBACK_STARTTIME = "repl.metrics.last.failback.starttime";
  public static final String REPL_METRICS_LAST_FAILBACK_ENDTIME = "repl.metrics.last.failback.endtime";
}
