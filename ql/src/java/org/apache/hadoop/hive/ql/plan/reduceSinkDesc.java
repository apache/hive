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

import java.io.Serializable;

@explain(displayName="Reduce Output Operator")
public class reduceSinkDesc implements Serializable {
  private static final long serialVersionUID = 1L;
  /**
   * Key columns are passed to reducer in the "key". 
   */
  private java.util.ArrayList<exprNodeDesc> keyCols;
  private java.util.ArrayList<java.lang.String> outputKeyColumnNames;
  /**
   * Value columns are passed to reducer in the "value". 
   */
  private java.util.ArrayList<exprNodeDesc> valueCols;
  private java.util.ArrayList<java.lang.String> outputValueColumnNames;
  /** 
   * Describe how to serialize the key.
   */
  private tableDesc keySerializeInfo;
  /**
   * Describe how to serialize the value.
   */
  private tableDesc valueSerializeInfo;
  
  /**
   * The tag for this reducesink descriptor.
   */
  private int tag;
  
  /**
   * The partition columns (CLUSTER BY or DISTRIBUTE BY in Hive language).
   * Partition columns decide the reducer that the current row goes to.
   * Partition columns are not passed to reducer.
   */
  private java.util.ArrayList<exprNodeDesc> partitionCols;
  
  private int numReducers;

  public reduceSinkDesc() { }

  public reduceSinkDesc
    (java.util.ArrayList<exprNodeDesc> keyCols,
     java.util.ArrayList<exprNodeDesc> valueCols,
     java.util.ArrayList<java.lang.String> outputKeyColumnNames,
     java.util.ArrayList<java.lang.String> outputValueolumnNames,
     int tag,
     java.util.ArrayList<exprNodeDesc> partitionCols,
     int numReducers,
     final tableDesc keySerializeInfo,
     final tableDesc valueSerializeInfo) {
    this.keyCols = keyCols;
    this.valueCols = valueCols;
    this.outputKeyColumnNames = outputKeyColumnNames;
    this.outputValueColumnNames = outputValueolumnNames;
    this.tag = tag;
    this.numReducers = numReducers;
    this.partitionCols = partitionCols;
    this.keySerializeInfo = keySerializeInfo;
    this.valueSerializeInfo = valueSerializeInfo;
  }

  public java.util.ArrayList<java.lang.String> getOutputKeyColumnNames() {
    return outputKeyColumnNames;
  }

  public void setOutputKeyColumnNames(
      java.util.ArrayList<java.lang.String> outputKeyColumnNames) {
    this.outputKeyColumnNames = outputKeyColumnNames;
  }

  public java.util.ArrayList<java.lang.String> getOutputValueColumnNames() {
    return outputValueColumnNames;
  }

  public void setOutputValueColumnNames(
      java.util.ArrayList<java.lang.String> outputValueColumnNames) {
    this.outputValueColumnNames = outputValueColumnNames;
  }

  @explain(displayName="key expressions")
  public java.util.ArrayList<exprNodeDesc> getKeyCols() {
    return this.keyCols;
  }
  public void setKeyCols
    (final java.util.ArrayList<exprNodeDesc> keyCols) {
    this.keyCols=keyCols;
  }

  @explain(displayName="value expressions")
  public java.util.ArrayList<exprNodeDesc> getValueCols() {
    return this.valueCols;
  }
  public void setValueCols
    (final java.util.ArrayList<exprNodeDesc> valueCols) {
    this.valueCols=valueCols;
  }
  
  @explain(displayName="Map-reduce partition columns")
  public java.util.ArrayList<exprNodeDesc> getPartitionCols() {
    return this.partitionCols;
  }
  public void setPartitionCols(final java.util.ArrayList<exprNodeDesc> partitionCols) {
    this.partitionCols = partitionCols;
  }
  
  @explain(displayName="tag")
  public int getTag() {
    return this.tag;
  }
  public void setTag(int tag) {
    this.tag = tag;
  }

  /**
   * Returns the number of reducers for the map-reduce job.
   * -1 means to decide the number of reducers at runtime. This enables Hive to estimate 
   * the number of reducers based on the map-reduce input data size, which is only 
   * available right before we start the map-reduce job.    
   */
  public int getNumReducers() {
    return this.numReducers;
  }
  public void setNumReducers(int numReducers) {
    this.numReducers = numReducers;
  }

  public tableDesc getKeySerializeInfo() {
    return keySerializeInfo;
  }

  public void setKeySerializeInfo(tableDesc keySerializeInfo) {
    this.keySerializeInfo = keySerializeInfo;
  }

  public tableDesc getValueSerializeInfo() {
    return valueSerializeInfo;
  }

  public void setValueSerializeInfo(tableDesc valueSerializeInfo) {
    this.valueSerializeInfo = valueSerializeInfo;
  }

  /**
   * Returns the sort order of the key columns.
   * @return null, which means ascending order for all key columns,
   *   or a String of the same length as key columns, that consists of only 
   *   "+" (ascending order) and "-" (descending order). 
   */
  @explain(displayName="sort order")
  public String getOrder() {
    return keySerializeInfo.getProperties().getProperty(
        org.apache.hadoop.hive.serde.Constants.SERIALIZATION_SORT_ORDER);
  }
  
}
