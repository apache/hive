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

import java.io.IOException;

import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.io.DataOutputBuffer;

@SuppressWarnings("serial")
@Explain(displayName = "Dynamic Partitioning Event Operator")
public class DynamicPruningEventDesc extends AppMasterEventDesc {

  // column in the target table that will be pruned against
  private String targetColumnName;

  // tableScan is only available during compile
  private transient TableScanOperator tableScan;

  // the partition column we're interested in
  private ExprNodeDesc partKey;

  public TableScanOperator getTableScan() {
    return tableScan;
  }

  public void setTableScan(TableScanOperator tableScan) {
    this.tableScan = tableScan;
  }

  @Explain(displayName = "Target column")
  public String getTargetColumnName() {
    return targetColumnName;
  }

  public void setTargetColumnName(String columnName) {
    this.targetColumnName = columnName;
  }

  @Override
  public void writeEventHeader(DataOutputBuffer buffer) throws IOException {
    super.writeEventHeader(buffer);
    buffer.writeUTF(targetColumnName);
  }

  public void setPartKey(ExprNodeDesc partKey) {
    this.partKey = partKey;
  }

  @Explain(displayName = "Partition key expr")
  public String getPartKeyString() {
    return this.partKey.getExprString();
  }

  public ExprNodeDesc getPartKey() {
    return this.partKey;
  }
}
