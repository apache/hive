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

package org.apache.hadoop.hive.ql.ddl.table.partition.exchange;

import java.util.Map;

import org.apache.hadoop.hive.ql.ddl.DDLDesc;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

/**
 * DDL task description for ALTER TABLE ... EXCHANGE PARTITION ... WITH TABLE ... commands.
 */
@Explain(displayName = "Exchange Partitions", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class AlterTableExchangePartitionsDesc implements DDLDesc {
  private final Table sourceTable;
  private final Table destinationTable;
  private final Map<String, String> partitionSpecs;

  public AlterTableExchangePartitionsDesc(Table sourceTable, Table destinationTable,
      Map<String, String> partitionSpecs) {
    this.sourceTable = sourceTable;
    this.destinationTable = destinationTable;
    this.partitionSpecs = partitionSpecs;
  }

  public Table getSourceTable() {
    return sourceTable;
  }

  public Table getDestinationTable() {
    return destinationTable;
  }

  @Explain(displayName = "partitions", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public Map<String, String> getPartitionSpecs() {
    return partitionSpecs;
  }
}
