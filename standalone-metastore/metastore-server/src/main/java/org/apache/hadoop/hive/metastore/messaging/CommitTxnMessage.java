/* * Licensed to the Apache Software Foundation (ASF) under one
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

package org.apache.hadoop.hive.metastore.messaging;

import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.WriteEventInfo;

import java.util.List;

/**
 * HCat message sent when an commit transaction is done.
 */
public abstract class CommitTxnMessage extends EventMessage {

  protected CommitTxnMessage() {
    super(EventType.COMMIT_TXN);
  }

  /**
   * Get the transaction id to be committed.
   *
   * @return The TxnId
   */
  public abstract Long getTxnId();

  public abstract List<Long> getWriteIds();

  public abstract List<String> getDatabases();

  public abstract List<String> getTables();

  public abstract List<String> getPartitions();

  public abstract Table getTableObj(int idx) throws Exception;

  public abstract Partition getPartitionObj(int idx) throws Exception;

  public abstract String getFiles(int idx);

  public abstract List<String> getFilesList();

  public abstract void addWriteEventInfo(List<WriteEventInfo> writeEventInfoList);
}
