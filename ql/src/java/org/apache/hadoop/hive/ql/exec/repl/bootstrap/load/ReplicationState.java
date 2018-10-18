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
package org.apache.hadoop.hive.ql.exec.repl.bootstrap.load;

import org.apache.hadoop.hive.ql.plan.AddPartitionDesc;

import java.io.Serializable;

public class ReplicationState implements Serializable {

  public static class PartitionState {
    final String tableName;
    public final AddPartitionDesc lastReplicatedPartition;

    public PartitionState(String tableName, AddPartitionDesc lastReplicatedPartition) {
      this.tableName = tableName;
      this.lastReplicatedPartition = lastReplicatedPartition;
    }
  }

  // null :: for non - partitioned table.
  public final PartitionState partitionState;
  // for non partitioned table this will represent the last tableName replicated, else its the name of the
  // current partitioned table with last partition replicated denoted by "lastPartitionReplicated"
  public final String lastTableReplicated;
  // last function name is replicated, null if function replication was in progress when we created this state.
  public final String functionName;

  public ReplicationState(PartitionState partitionState) {
    this.partitionState = partitionState;
    this.functionName = null;
    this.lastTableReplicated = null;
  }

  @Override
  public String toString() {
    return "ReplicationState{" +
        ", partitionState=" + partitionState +
        ", lastTableReplicated='" + lastTableReplicated + '\'' +
        ", functionName='" + functionName + '\'' +
        '}';
  }
}
