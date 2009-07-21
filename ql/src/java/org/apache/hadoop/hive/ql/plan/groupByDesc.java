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

@explain(displayName="Group By Operator")
public class groupByDesc implements java.io.Serializable {
  /** Group-by Mode:
   *  COMPLETE: complete 1-phase aggregation: iterate, terminate
   *  PARTIAL1: partial aggregation - first phase:  iterate, terminatePartial
   *  PARTIAL2: partial aggregation - second phase: merge, terminatePartial
   *  PARTIALS: For non-distinct the same as PARTIAL2, for distinct the same as PARTIAL1
   *  FINAL: partial aggregation - final phase: merge, terminate
   *  HASH: For non-distinct the same as PARTIAL1 but use hash-table-based aggregation
   *  MERGEPARTIAL: FINAL for non-distinct aggregations, COMPLETE for distinct aggregations  
   */
  private static final long serialVersionUID = 1L;
  public static enum Mode { COMPLETE, PARTIAL1, PARTIAL2, PARTIALS, FINAL, HASH, MERGEPARTIAL };
  private Mode mode;
  private boolean groupKeyNotReductionKey;

  private java.util.ArrayList<exprNodeDesc> keys;
  private java.util.ArrayList<org.apache.hadoop.hive.ql.plan.aggregationDesc> aggregators;
  private java.util.ArrayList<java.lang.String> outputColumnNames;
  public groupByDesc() { }
  public groupByDesc(
    final Mode mode,
    final java.util.ArrayList<java.lang.String> outputColumnNames,
    final java.util.ArrayList<exprNodeDesc> keys,
    final java.util.ArrayList<org.apache.hadoop.hive.ql.plan.aggregationDesc> aggregators,
    final boolean groupKeyNotReductionKey) {
    this.mode = mode;
    this.outputColumnNames = outputColumnNames;
    this.keys = keys;
    this.aggregators = aggregators;
    this.groupKeyNotReductionKey = groupKeyNotReductionKey;
  }
  public Mode getMode() {
    return this.mode;
  }
  @explain(displayName="mode")
  public String getModeString() {
    switch(mode) {
    case COMPLETE:
      return "complete";
    case PARTIAL1:
      return "partial1";
    case PARTIAL2:
      return "partial2";
    case PARTIALS:
      return "partials";
    case HASH:
      return "hash";
    case FINAL:
      return "final";
    case MERGEPARTIAL:
      return "mergepartial";
    }
  
    return "unknown";
  }
  public void setMode(final Mode mode) {
    this.mode = mode;
  }
  @explain(displayName="keys")
  public java.util.ArrayList<exprNodeDesc> getKeys() {
    return this.keys;
  }
  public void setKeys(final java.util.ArrayList<exprNodeDesc> keys) {
    this.keys = keys;
  }
  
  public java.util.ArrayList<java.lang.String> getOutputColumnNames() {
    return outputColumnNames;
  }
  public void setOutputColumnNames(
      java.util.ArrayList<java.lang.String> outputColumnNames) {
    this.outputColumnNames = outputColumnNames;
  }
  
  @explain(displayName="aggregations")
  public java.util.ArrayList<org.apache.hadoop.hive.ql.plan.aggregationDesc> getAggregators() {
    return this.aggregators;
  }
  public void setAggregators(final java.util.ArrayList<org.apache.hadoop.hive.ql.plan.aggregationDesc> aggregators) {
    this.aggregators = aggregators;
  }


  public boolean getGroupKeyNotReductionKey() {
    return this.groupKeyNotReductionKey;
  }
  public void setGroupKeyNotReductionKey(final boolean groupKeyNotReductionKey) {
    this.groupKeyNotReductionKey = groupKeyNotReductionKey;
  }
}
