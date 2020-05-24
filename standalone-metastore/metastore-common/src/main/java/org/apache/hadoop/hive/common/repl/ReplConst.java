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
}
