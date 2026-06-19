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

package org.apache.hadoop.hive.ql.plan;

import java.io.IOException;
import java.util.Objects;

import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.optimizer.signature.Signature;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.hive.ql.plan.Explain.Level;


@SuppressWarnings("serial")
@Explain(displayName = "Dynamic Partitioning Event Operator", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class DynamicPruningEventDesc extends AppMasterEventDesc {

  // column in the target table that will be pruned against
  private String targetColumnName;

  // type of target column
  private String targetColumnType;

  // tableScan is only available during compile
  private transient TableScanOperator tableScan;

  // reduceSink is only available during compile
  private transient ReduceSinkOperator generator;

  // the partition column we're interested in
  private ExprNodeDesc partKey;

  private ExprNodeDesc predicate;

  public ExprNodeDesc getPredicate() {
    return predicate;
  }

  public void setPredicate(ExprNodeDesc predicate) {
    this.predicate = predicate;
  }

  public String getPartPredicateString() {
    return this.predicate != null ? this.predicate.getExprString() : "-";
  }

  public TableScanOperator getTableScan() {
    return tableScan;
  }

  public void setTableScan(TableScanOperator tableScan) {
    this.tableScan = tableScan;
  }

  public ReduceSinkOperator getGenerator() {
    return generator;
  }

  public void setGenerator(ReduceSinkOperator generator) {
    this.generator = generator;
  }

  @Explain(displayName = "Target column")
  public String displayTargetColumn() {
    return targetColumnName + " (" + targetColumnType + ")";
  }

  @Signature
  public String getTargetColumnName() {
    return targetColumnName;
  }

  public void setTargetColumnName(String columnName) {
    this.targetColumnName = columnName;
  }

  @Signature
  public String getTargetColumnType() {
    return targetColumnType;
  }

  public void setTargetColumnType(String columnType) {
    this.targetColumnType = columnType;
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
  @Signature
  public String getPartKeyString() {
    return this.partKey.getExprString();
  }

  public ExprNodeDesc getPartKey() {
    return this.partKey;
  }

  @Override
  public boolean isSame(OperatorDesc other) {
    if (super.isSame(other)) {
      DynamicPruningEventDesc otherDesc = (DynamicPruningEventDesc) other;
      return Objects.equals(getTargetColumnName(), otherDesc.getTargetColumnName()) &&
          Objects.equals(getTargetColumnType(), otherDesc.getTargetColumnType()) &&
          Objects.equals(getPartKeyString(), otherDesc.getPartKeyString()) &&
          Objects.equals(getPartPredicateString(), otherDesc.getPartPredicateString());
    }
    return false;
  }

}
