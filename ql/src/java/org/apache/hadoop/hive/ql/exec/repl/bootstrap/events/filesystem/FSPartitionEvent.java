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
package org.apache.hadoop.hive.ql.exec.repl.bootstrap.events.filesystem;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.events.PartitionEvent;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.events.TableEvent;
import org.apache.hadoop.hive.ql.exec.repl.bootstrap.load.ReplicationState;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.AddPartitionDesc;
import org.apache.hadoop.hive.ql.plan.ImportTableDesc;

import java.util.List;

public class FSPartitionEvent implements PartitionEvent {

  private final ReplicationState replicationState;
  private final TableEvent tableEvent;

  FSPartitionEvent(HiveConf hiveConf, String metadataDir,
      ReplicationState replicationState) {
    tableEvent = new FSTableEvent(hiveConf, metadataDir);
    this.replicationState = replicationState;
  }

  @Override
  public EventType eventType() {
    return EventType.Partition;
  }

  @Override
  public AddPartitionDesc lastPartitionReplicated() {
    assert replicationState != null && replicationState.partitionState != null;
    return replicationState.partitionState.lastReplicatedPartition;
  }

  @Override
  public TableEvent asTableEvent() {
    return tableEvent;
  }

  @Override
  public ImportTableDesc tableDesc(String dbName) throws SemanticException {
    return tableEvent.tableDesc(dbName);
  }

  @Override
  public List<AddPartitionDesc> partitionDescriptions(ImportTableDesc tblDesc)
      throws SemanticException {
    return tableEvent.partitionDescriptions(tblDesc);
  }

  @Override
  public ReplicationSpec replicationSpec() {
    return tableEvent.replicationSpec();
  }

  @Override
  public boolean shouldNotReplicate() {
    return tableEvent.shouldNotReplicate();
  }

  @Override
  public Path metadataPath() {
    return tableEvent.metadataPath();
  }
}
