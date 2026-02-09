/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg;

import org.apache.iceberg.hive.HiveTableOperations;
import org.apache.iceberg.hive.StagingTableOperations;

/**
 * Transaction implementation that stages metadata changes for atomic batch HMS updates across
 * multiple tables.
 *
 * <p>Extends BaseTransaction to leverage Iceberg's retry and conflict resolution logic while
 * capturing metadata locations instead of publishing directly to HMS.
 */
public class HiveTransaction extends BaseTransaction {

  private final HiveTableOperations hiveOps;
  private final StagingTableOperations stagingOps;

  public HiveTransaction(Table table, HiveTableOperations ops) {
    this(table, ops, ops.toStagingOps());
  }

  private HiveTransaction(Table table, HiveTableOperations ops, StagingTableOperations stagingOps) {
    super(table.name(), stagingOps, TransactionType.SIMPLE, ops.current());
    this.hiveOps = ops;
    this.stagingOps = stagingOps;
  }

  public HiveTableOperations ops() {
    return hiveOps;
  }

  public StagingTableOperations stagingOps() {
    return stagingOps;
  }
}
