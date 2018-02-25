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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorUtils;
import org.apache.hadoop.hive.ql.plan.Explain.Level;
import org.apache.hadoop.hive.ql.plan.Explain.Vectorization;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
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

  // boolean that says whether tez auto reduce parallelism should be used
  private boolean isAutoReduceParallelism;
  // boolean that says whether the data distribution is uniform hash (not java HashCode)
  private transient boolean isUniformDistribution = false;

  // boolean that says whether to slow start or not
  private boolean isSlowStart = true;

  // for auto reduce parallelism - minimum reducers requested
  private int minReduceTasks;

  // for auto reduce parallelism - max reducers requested
  private int maxReduceTasks;

  private ObjectInspector keyObjectInspector = null;
  private ObjectInspector valueObjectInspector = null;

  private boolean reduceVectorizationEnabled;
  private String vectorReduceEngine;

  private String vectorReduceColumnSortOrder;
  private String vectorReduceColumnNullOrder;

  private transient TezEdgeProperty edgeProp;

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

  @Explain(displayName = "Execution mode", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED },
      vectorization = Vectorization.SUMMARY_PATH)
  public String getExecutionMode() {
    if (vectorMode) {
      if (llapMode) {
	if (uberMode) {
	  return "vectorized, uber";
	} else {
	  return "vectorized, llap";
	}
      } else {
	return "vectorized";
      }
    } else if (llapMode) {
      return uberMode? "uber" : "llap";
    }
    return null;
  }

  @Explain(displayName = "Reduce Operator Tree", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED },
      vectorization = Vectorization.OPERATOR_PATH)
  public Operator<?> getReducer() {
    return reducer;
  }

  public void setReducer(final Operator<?> reducer) {
    this.reducer = reducer;
  }

  @Explain(displayName = "Needs Tagging", explainLevels = { Level.EXTENDED })
  public boolean getNeedsTagging() {
    return needsTagging;
  }

  public void setNeedsTagging(boolean needsTagging) {
    this.needsTagging = needsTagging;
  }

  public void setTagToInput(final Map<Integer, String> tagToInput) {
    this.tagToInput = tagToInput;
  }

  @Explain(displayName = "tagToInput", explainLevels = { Level.USER })
  public Map<Integer, String> getTagToInput() {
    return tagToInput;
  }

  @Override
  public void replaceRoots(Map<Operator<?>, Operator<?>> replacementMap) {
    setReducer(replacementMap.get(getReducer()));
  }

  @Override
  public Set<Operator<?>> getAllRootOperators() {
    Set<Operator<?>> opSet = new LinkedHashSet<Operator<?>>();
    opSet.add(getReducer());
    return opSet;
  }

  @Override
  public Operator<? extends OperatorDesc> getAnyRootOperator() {
    return getReducer();
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

  @Override
  public void configureJobConf(JobConf job) {
    if (reducer != null) {
      for (FileSinkOperator fs : OperatorUtils.findOperators(reducer, FileSinkOperator.class)) {
        PlanUtils.configureJobConf(fs.getConf().getTableInfo(), job);
      }
    }
  }

  public void setAutoReduceParallelism(boolean isAutoReduceParallelism) {
    this.isAutoReduceParallelism = isAutoReduceParallelism;
  }

  public boolean isAutoReduceParallelism() {
    return isAutoReduceParallelism;
  }

  public boolean isSlowStart() {
    return isSlowStart;
  }

  public void setSlowStart(boolean isSlowStart) {
    this.isSlowStart = isSlowStart;
  }

  // ReducerTraits.UNIFORM
  public void setUniformDistribution(boolean isUniformDistribution) {
    this.isUniformDistribution = isUniformDistribution;
  }

  public boolean isUniformDistribution() {
    return this.isUniformDistribution;
  }

  public void setMinReduceTasks(int minReduceTasks) {
    this.minReduceTasks = minReduceTasks;
  }

  public int getMinReduceTasks() {
    return minReduceTasks;
  }

  public int getMaxReduceTasks() {
    return maxReduceTasks;
  }

  public void setMaxReduceTasks(int maxReduceTasks) {
    this.maxReduceTasks = maxReduceTasks;
  }

  public void setReduceVectorizationEnabled(boolean reduceVectorizationEnabled) {
    this.reduceVectorizationEnabled = reduceVectorizationEnabled;
  }

  public boolean getReduceVectorizationEnabled() {
    return reduceVectorizationEnabled;
  }

  public void setVectorReduceEngine(String vectorReduceEngine) {
    this.vectorReduceEngine = vectorReduceEngine;
  }

  public String getVectorReduceEngine() {
    return vectorReduceEngine;
  }

  public void setVectorReduceColumnSortOrder(String vectorReduceColumnSortOrder) {
    this.vectorReduceColumnSortOrder = vectorReduceColumnSortOrder;
  }

  public String getVectorReduceColumnSortOrder() {
    return vectorReduceColumnSortOrder;
  }

  public void setVectorReduceColumnNullOrder(String vectorReduceColumnNullOrder) {
    this.vectorReduceColumnNullOrder = vectorReduceColumnNullOrder;
  }

  public String getVectorReduceColumnNullOrder() {
    return vectorReduceColumnNullOrder;
  }

  // Use LinkedHashSet to give predictable display order.
  private static Set<String> reduceVectorizableEngines =
      new LinkedHashSet<String>(Arrays.asList("tez", "spark"));

  public class ReduceExplainVectorization extends BaseExplainVectorization {

    private final ReduceWork reduceWork;

    private VectorizationCondition[] reduceVectorizationConditions;

    public ReduceExplainVectorization(ReduceWork reduceWork) {
      super(reduceWork);
      this.reduceWork = reduceWork;
    }

    private VectorizationCondition[] createReduceExplainVectorizationConditions() {

      boolean enabled = reduceWork.getReduceVectorizationEnabled();

      String engine = reduceWork.getVectorReduceEngine();
      String engineInSupportedCondName =
          HiveConf.ConfVars.HIVE_EXECUTION_ENGINE.varname + " " + engine + " IN " + reduceVectorizableEngines;

      boolean engineInSupported = reduceVectorizableEngines.contains(engine);

      VectorizationCondition[] conditions = new VectorizationCondition[] {
          new VectorizationCondition(
              enabled,
              HiveConf.ConfVars.HIVE_VECTORIZATION_REDUCE_ENABLED.varname),
          new VectorizationCondition(
              engineInSupported,
              engineInSupportedCondName)
      };
      return conditions;
    }

    @Explain(vectorization = Vectorization.SUMMARY, displayName = "enableConditionsMet", explainLevels = { Level.DEFAULT, Level.EXTENDED })
    public List<String> getEnableConditionsMet() {
      if (reduceVectorizationConditions == null) {
        reduceVectorizationConditions = createReduceExplainVectorizationConditions();
      }
      return VectorizationCondition.getConditionsMet(reduceVectorizationConditions);
    }

    @Explain(vectorization = Vectorization.SUMMARY, displayName = "enableConditionsNotMet", explainLevels = { Level.DEFAULT, Level.EXTENDED })
    public List<String> getEnableConditionsNotMet() {
      if (reduceVectorizationConditions == null) {
        reduceVectorizationConditions = createReduceExplainVectorizationConditions();
      }
      return VectorizationCondition.getConditionsNotMet(reduceVectorizationConditions);
    }

    @Explain(vectorization = Vectorization.DETAIL, displayName = "reduceColumnSortOrder", explainLevels = { Level.DEFAULT, Level.EXTENDED })
    public String getReduceColumnSortOrder() {
      if (!getVectorizationExamined()) {
        return null;
      }
      return reduceWork.getVectorReduceColumnSortOrder();
    }

    @Explain(vectorization = Vectorization.DETAIL, displayName = "reduceColumnNullOrder", explainLevels = { Level.DEFAULT, Level.EXTENDED })
    public String getReduceColumnNullOrder() {
      if (!getVectorizationExamined()) {
        return null;
      }
      return reduceWork.getVectorReduceColumnNullOrder();
    }
  }

  @Explain(vectorization = Vectorization.SUMMARY, displayName = "Reduce Vectorization", explainLevels = { Level.DEFAULT, Level.EXTENDED })
  public ReduceExplainVectorization getReduceExplainVectorization() {
    if (!getVectorizationExamined()) {
      return null;
    }
    return new ReduceExplainVectorization(this);
  }

  public void setEdgePropRef(TezEdgeProperty edgeProp) {
    this.edgeProp = edgeProp;
  }

  public TezEdgeProperty getEdgePropRef() {
    return edgeProp;
  }
}
