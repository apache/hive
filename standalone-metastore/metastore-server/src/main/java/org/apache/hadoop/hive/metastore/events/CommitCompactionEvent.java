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

package org.apache.hadoop.hive.metastore.events;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.txn.entities.CompactionInfo;

/**
 * CommitCompactionEvent
 * Event generated for compaction commit operation
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class CommitCompactionEvent extends ListenerEvent {

  private final Long txnId;
  private final Long compactionId;
  private final CompactionType type;
  private final String dbname;
  private final String tableName;
  private final String partName;

  public CommitCompactionEvent(Long txnId, CompactionInfo ci) {
    this(txnId, ci.id, ci.type, ci.dbname, ci.tableName, ci.partName, null);
  }

  public CommitCompactionEvent(Long txnId, CompactionInfo ci, IHMSHandler handler) {
    this(txnId, ci.id, ci.type, ci.dbname, ci.tableName, ci.partName, handler);
  }

  public CommitCompactionEvent(Long txnId, Long compactionId, CompactionType type, String dbname, String tableName,
      String partName, IHMSHandler handler) {
    super(true, handler);
    this.txnId = txnId;
    this.compactionId = compactionId;
    this.type = type;
    this.dbname = dbname;
    this.tableName = tableName;
    this.partName = partName;
  }

  public Long getTxnId() {
    return txnId;
  }

  public Long getCompactionId() {
    return compactionId;
  }

  public CompactionType getType() {
    return type;
  }

  public String getDbname() {
    return dbname;
  }

  public String getTableName() {
    return tableName;
  }

  public String getPartName() {
    return partName;
  }
}
