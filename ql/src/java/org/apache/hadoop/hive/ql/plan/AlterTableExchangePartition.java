/**
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

package org.apache.hadoop.hive.ql.plan;

import java.util.Map;

import org.apache.hadoop.hive.ql.metadata.Table;

public class AlterTableExchangePartition extends DDLDesc {

  // The source table
  private Table sourceTable;

  // The destination table
  private Table destinationTable;

  // The partition that has to be exchanged
  private Map<String, String> partitionSpecs;

  public AlterTableExchangePartition(Table sourceTable, Table destinationTable,
      Map<String, String> partitionSpecs) {
    super();
    this.sourceTable = sourceTable;
    this.destinationTable = destinationTable;
    this.partitionSpecs = partitionSpecs;
  }

  public void setSourceTable(Table sourceTable) {
    this.sourceTable = sourceTable;
  }

  public Table getSourceTable() {
    return this.sourceTable;
  }

  public void setDestinationTable(Table destinationTable) {
    this.destinationTable = destinationTable;
  }

  public Table getDestinationTable() {
    return this.destinationTable;
  }

  public void setPartitionSpecs(Map<String, String> partitionSpecs) {
    this.partitionSpecs = partitionSpecs;
  }

  public Map<String, String> getPartitionSpecs() {
    return this.partitionSpecs;
  }
}
