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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.LinkedHashSet;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorUtils;
import org.apache.hadoop.mapred.JobConf;

/**
 * ReduceWork represents all the information used to run a reduce task on the cluster.
 * It is first used when the query planner breaks the logical plan into tasks and
 * used throughout physical optimization to track reduce-side operator plans, schema
 * info about key/value pairs, etc
 *
 * ExecDriver will serialize the contents of this class and make sure it is
 * distributed on the cluster. The ExecReducer will ultimately deserialize this
 * class on the data nodes and setup it's operator pipeline accordingly.
 *
 * This class is also used in the explain command any property with the 
 * appropriate annotation will be displayed in the explain output.
 */
@SuppressWarnings({"serial", "deprecation"})
public class ReduceWork extends BaseWork {

  public ReduceWork() {}

  public ReduceWork(String name) {
    super(name);
  }

  private static transient final Log LOG = LogFactory.getLog(ReduceWork.class);

  // schema of the map-reduce 'key' object - this is homogeneous
  private TableDesc keyDesc;

  // schema of the map-reduce 'value' object - this is heterogeneous
  private List<TableDesc> tagToValueDesc = new ArrayList<TableDesc>();

  // first operator of the reduce task. (not the reducesinkoperator, but the
  // operator that handles the output of these, e.g.: JoinOperator).
  private Operator<?> reducer;

  // desired parallelism of the reduce task.
  private Integer numReduceTasks;

  // boolean to signal whether tagging will be used (e.g.: join) or 
  // not (e.g.: group by)
  private boolean needsTagging;

  private Map<Integer, String> tagToInput = new HashMap<Integer, String>();

  /**
   * If the plan has a reducer and correspondingly a reduce-sink, then store the TableDesc pointing
   * to keySerializeInfo of the ReduceSink
   *
   * @param keyDesc
   */
  public void setKeyDesc(final TableDesc keyDesc) {
    this.keyDesc = keyDesc;
  }

  public TableDesc getKeyDesc() {
    return keyDesc;
  }

  public List<TableDesc> getTagToValueDesc() {
    return tagToValueDesc;
  }

  public void setTagToValueDesc(final List<TableDesc> tagToValueDesc) {
    this.tagToValueDesc = tagToValueDesc;
  }

  @Explain(displayName = "Execution mode")
  public String getVectorModeOn() {
    return vectorMode ? "vectorized" : null;
  }

  @Explain(displayName = "Reduce Operator Tree")
  public Operator<?> getReducer() {
    return reducer;
  }

  public void setReducer(final Operator<?> reducer) {
    this.reducer = reducer;
  }

  @Explain(displayName = "Needs Tagging", normalExplain = false)
  public boolean getNeedsTagging() {
    return needsTagging;
  }

  public void setNeedsTagging(boolean needsTagging) {
    this.needsTagging = needsTagging;
  }

  public void setTagToInput(final Map<Integer, String> tagToInput) {
    this.tagToInput = tagToInput;
  }

  public Map<Integer, String> getTagToInput() {
    return tagToInput;
  }

  @Override
  public void replaceRoots(Map<Operator<?>, Operator<?>> replacementMap) {
    assert replacementMap.size() == 1;
    setReducer(replacementMap.get(getReducer()));
  }

  @Override
  public Set<Operator<?>> getAllRootOperators() {
    Set<Operator<?>> opSet = new LinkedHashSet<Operator<?>>();
    opSet.add(getReducer());
    return opSet;
  }

  /**
   * If the number of reducers is -1, the runtime will automatically figure it
   * out by input data size.
   *
   * The number of reducers will be a positive number only in case the target
   * table is bucketed into N buckets (through CREATE TABLE). This feature is
   * not supported yet, so the number of reducers will always be -1 for now.
   */
  public Integer getNumReduceTasks() {
      return numReduceTasks;
  }

  public void setNumReduceTasks(final Integer numReduceTasks) {
    this.numReduceTasks = numReduceTasks;
  }

  public void configureJobConf(JobConf job) {
    if (reducer != null) {
      for (FileSinkOperator fs : OperatorUtils.findOperators(reducer, FileSinkOperator.class)) {
        PlanUtils.configureJobConf(fs.getConf().getTableInfo(), job);
      }
    }
  }
}
