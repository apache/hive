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


import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.PTFUtils;
import org.apache.hadoop.hive.ql.plan.Explain.Level;
import org.apache.hadoop.hive.ql.stats.StatsCollectionContext;

public class AbstractOperatorDesc implements OperatorDesc {

  protected boolean vectorMode = false;

  // Reference to vectorization description needed for EXPLAIN VECTORIZATION, hash table loading,
  // etc.
  protected VectorDesc vectorDesc;

  protected Statistics statistics;
  protected transient OpTraits opTraits;
  protected transient Map<String, String> opProps;
  protected long memNeeded = 0;
  protected long memAvailable = 0;
  protected String runtimeStatsTmpDir;

  /**
   * A map of output column name to input expression map. This is used by
   * optimizer and built during semantic analysis contains only key elements for
   * reduce sink and group by op
   */
  protected Map<String, ExprNodeDesc> colExprMap;

  @Override
  @Explain(skipHeader = true, displayName = "Statistics")
  public Statistics getStatistics() {
    return statistics;
  }

  @Explain(skipHeader = true, displayName = "Statistics", explainLevels = { Level.USER })
  public String getUserLevelStatistics() {
    return statistics.toUserLevelExplainString();
  }

  @Override
  public void setStatistics(Statistics statistics) {
    this.statistics = statistics;
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    throw new CloneNotSupportedException("clone not supported");
  }

  public boolean getVectorMode() {
    return vectorMode;
  }

  public void setVectorMode(boolean vm) {
    this.vectorMode = vm;
  }

  public void setVectorDesc(VectorDesc vectorDesc) {
    this.vectorDesc = vectorDesc;
  }

  public VectorDesc getVectorDesc() {
    return vectorDesc;
  }

  @Override
  public OpTraits getTraits() {
    return opTraits;
  }

  @Override
  public void setTraits(OpTraits opTraits) {
    this.opTraits = opTraits;
  }

  @Override
  public Map<String, String> getOpProps() {
    return opProps;
  }

  public void setOpProps(Map<String, String> props) {
    this.opProps = props;
  }

  @Override
  public long getMemoryNeeded() {
    return memNeeded;
  }

  @Override
  public void setMemoryNeeded(long memNeeded) {
    this.memNeeded = memNeeded;
  }

  @Override
  public long getMaxMemoryAvailable() {
    return memAvailable;
  }

  @Override
  public void setMaxMemoryAvailable(final long memoryAvailble) {
    this.memAvailable = memoryAvailble;
  }

  public String getRuntimeStatsTmpDir() {
    return runtimeStatsTmpDir;
  }

  public void setRuntimeStatsTmpDir(String runtimeStatsTmpDir) {
    this.runtimeStatsTmpDir = runtimeStatsTmpDir;
  }

  /**
   * The default implementation delegates to {@link #equals(Object)}. Intended to be
   * overridden by sub classes.
   */
  @Override
  public boolean isSame(OperatorDesc other) {
    return equals(other);
  }

  @Explain(displayName = "columnExprMap", jsonOnly = true)
  public Map<String, String> getColumnExprMapForExplain() {
    Map<String, String> colExprMapForExplain = new HashMap<>();
    for(String col:this.colExprMap.keySet()) {
      colExprMapForExplain.put(col, this.colExprMap.get(col).getExprString());
    }
    return colExprMapForExplain;
  }

  @Override
  public Map<String, ExprNodeDesc> getColumnExprMap() {
    return this.colExprMap;
  }

  @Override
  public void setColumnExprMap(Map<String, ExprNodeDesc> colExprMap) {
    this.colExprMap = colExprMap;
  }

}
