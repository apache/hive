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

package org.apache.hadoop.hive.ql.optimizer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.exec.DependencyCollectionTask;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.UnionOperator;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.plan.DependencyCollectionWork;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.MoveWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;

/**
 * Processor Context for creating map reduce task. Walk the tree in a DFS manner
 * and process the nodes. Some state is maintained about the current nodes
 * visited so far.
 */
public class GenMRProcContext implements NodeProcessorCtx {

  /**
   * GenMapRedCtx is used to keep track of the current state.
   */
  public static class GenMapRedCtx {
    Task<? extends Serializable> currTask;
    String currAliasId;

    public GenMapRedCtx() {
    }

    /**
     * @param currTask
     *          the current task
     * @param currAliasId
     */
    public GenMapRedCtx(Task<? extends Serializable> currTask, String currAliasId) {
      this.currTask = currTask;
      this.currAliasId = currAliasId;
    }

    /**
     * @return current task
     */
    public Task<? extends Serializable> getCurrTask() {
      return currTask;
    }

    /**
     * @return current alias
     */
    public String getCurrAliasId() {
      return currAliasId;
    }
  }

  /**
   * GenMRUnionCtx.
   *
   */
  public static class GenMRUnionCtx {
    final Task<? extends Serializable> uTask;
    List<String> taskTmpDir;
    List<TableDesc> tt_desc;
    List<TableScanOperator> listTopOperators;

    public GenMRUnionCtx(Task<? extends Serializable> uTask) {
      this.uTask = uTask;
      taskTmpDir = new ArrayList<String>();
      tt_desc = new ArrayList<TableDesc>();
      listTopOperators = new ArrayList<>();
    }

    public Task<? extends Serializable> getUTask() {
      return uTask;
    }

    public void addTaskTmpDir(String taskTmpDir) {
      this.taskTmpDir.add(taskTmpDir);
    }

    public List<String> getTaskTmpDir() {
      return taskTmpDir;
    }

    public void addTTDesc(TableDesc tt_desc) {
      this.tt_desc.add(tt_desc);
    }

    public List<TableDesc> getTTDesc() {
      return tt_desc;
    }

    public List<TableScanOperator> getListTopOperators() {
      return listTopOperators;
    }

    public void addListTopOperators(TableScanOperator topOperator) {
      listTopOperators.add(topOperator);
    }
  }

  private HiveConf conf;
  private
    HashMap<Operator<? extends OperatorDesc>, Task<? extends Serializable>> opTaskMap;
  private
    HashMap<Task<? extends Serializable>, List<Operator<? extends OperatorDesc>>> taskToSeenOps;

  private HashMap<UnionOperator, GenMRUnionCtx> unionTaskMap;
  private List<FileSinkOperator> seenFileSinkOps;

  private ParseContext parseCtx;
  private List<Task<MoveWork>> mvTask;
  private List<Task<? extends Serializable>> rootTasks;

  private LinkedHashMap<Operator<? extends OperatorDesc>, GenMapRedCtx> mapCurrCtx;
  private Task<? extends Serializable> currTask;
  private TableScanOperator currTopOp;
  private UnionOperator currUnionOp;
  private String currAliasId;
  private DependencyCollectionTask dependencyTaskForMultiInsert;

  // If many fileSinkDescs are linked to each other, it is a good idea to keep track of
  // tasks for first fileSinkDesc. others can use it
  private Map<FileSinkDesc, Task<? extends Serializable>> linkedFileDescTasks;

  /**
   * Set of read entities. This list is generated by the walker and is passed to
   * the hooks.
   */
  private Set<ReadEntity> inputs;
  /**
   * Set of write entities. This list is generated by the walker and is passed
   * to the hooks.
   */
  private Set<WriteEntity> outputs;

  public GenMRProcContext() {
  }

  /**
   * @param conf
   *          hive configuration
   * @param opTaskMap
   *          reducer to task mapping
   * @param seenOps
   *          operator already visited
   * @param parseCtx
   *          current parse context
   * @param rootTasks
   *          root tasks for the plan
   * @param mvTask
   *          the final move task
   * @param mapCurrCtx
   *          operator to task mappings
   * @param inputs
   *          the set of input tables/partitions generated by the walk
   * @param outputs
   *          the set of destinations generated by the walk
   */
  public GenMRProcContext(
      HiveConf conf,
      HashMap<Operator<? extends OperatorDesc>, Task<? extends Serializable>> opTaskMap,
      ParseContext parseCtx,
      List<Task<MoveWork>> mvTask,
      List<Task<? extends Serializable>> rootTasks,
      LinkedHashMap<Operator<? extends OperatorDesc>, GenMapRedCtx> mapCurrCtx,
      Set<ReadEntity> inputs, Set<WriteEntity> outputs) {
    this.conf = conf;
    this.opTaskMap = opTaskMap;
    this.mvTask = mvTask;
    this.parseCtx = parseCtx;
    this.rootTasks = rootTasks;
    this.mapCurrCtx = mapCurrCtx;
    this.inputs = inputs;
    this.outputs = outputs;
    currTask = null;
    currTopOp = null;
    currUnionOp = null;
    currAliasId = null;
    unionTaskMap = new HashMap<UnionOperator, GenMRUnionCtx>();
    taskToSeenOps = new HashMap<Task<? extends Serializable>,
        List<Operator<? extends OperatorDesc>>>();
    dependencyTaskForMultiInsert = null;
    linkedFileDescTasks = null;
  }

  /**
   * @return reducer to task mapping
   */
  public HashMap<Operator<? extends OperatorDesc>,
                 Task<? extends Serializable>> getOpTaskMap() {
    return opTaskMap;
  }

  /**
   * @param opTaskMap
   *          reducer to task mapping
   */
  public void setOpTaskMap(
    HashMap<Operator<? extends OperatorDesc>, Task<? extends Serializable>> opTaskMap) {
    this.opTaskMap = opTaskMap;
  }

  public boolean isSeenOp(Task task, Operator operator) {
    List<Operator<?extends OperatorDesc>> seenOps = taskToSeenOps.get(task);
    return seenOps != null && seenOps.contains(operator);
  }

  public void addSeenOp(Task task, Operator operator) {
    List<Operator<?extends OperatorDesc>> seenOps = taskToSeenOps.get(task);
    if (seenOps == null) {
      taskToSeenOps.put(task, seenOps = new ArrayList<Operator<? extends OperatorDesc>>());
    }
    seenOps.add(operator);
  }

  /**
   * @return file operators already visited
   */
  public List<FileSinkOperator> getSeenFileSinkOps() {
    return seenFileSinkOps;
  }

  /**
   * @param seenFileSinkOps
   *          file sink operators already visited
   */
  public void setSeenFileSinkOps(List<FileSinkOperator> seenFileSinkOps) {
    this.seenFileSinkOps = seenFileSinkOps;
  }

  /**
   * @return current parse context
   */
  public ParseContext getParseCtx() {
    return parseCtx;
  }

  /**
   * @param parseCtx
   *          current parse context
   */
  public void setParseCtx(ParseContext parseCtx) {
    this.parseCtx = parseCtx;
  }

  /**
   * @return the final move task
   */
  public List<Task<MoveWork>> getMvTask() {
    return mvTask;
  }

  /**
   * @param mvTask
   *          the final move task
   */
  public void setMvTask(List<Task<MoveWork>> mvTask) {
    this.mvTask = mvTask;
  }

  /**
   * @return root tasks for the plan
   */
  public List<Task<? extends Serializable>> getRootTasks() {
    return rootTasks;
  }

  /**
   * @param rootTasks
   *          root tasks for the plan
   */
  public void setRootTasks(List<Task<? extends Serializable>> rootTasks) {
    this.rootTasks = rootTasks;
  }

  public boolean addRootIfPossible(Task<? extends Serializable> task) {
    if (task.getParentTasks() == null || task.getParentTasks().isEmpty()) {
      if (!rootTasks.contains(task)) {
        return rootTasks.add(task);
      }
    }
    return false;
  }

  /**
   * @return operator to task mappings
   */
  public LinkedHashMap<Operator<? extends OperatorDesc>, GenMapRedCtx> getMapCurrCtx() {
    return mapCurrCtx;
  }

  /**
   * @param mapCurrCtx
   *          operator to task mappings
   */
  public void setMapCurrCtx(
      LinkedHashMap<Operator<? extends OperatorDesc>, GenMapRedCtx> mapCurrCtx) {
    this.mapCurrCtx = mapCurrCtx;
  }

  /**
   * @return current task
   */
  public Task<? extends Serializable> getCurrTask() {
    return currTask;
  }

  /**
   * @param currTask
   *          current task
   */
  public void setCurrTask(Task<? extends Serializable> currTask) {
    this.currTask = currTask;
  }

  /**
   * @return current top operator
   */
  public TableScanOperator getCurrTopOp() {
    return currTopOp;
  }

  /**
   * @param currTopOp
   *          current top operator
   */
  public void setCurrTopOp(TableScanOperator currTopOp) {
    this.currTopOp = currTopOp;
  }

  public UnionOperator getCurrUnionOp() {
    return currUnionOp;
  }

  /**
   * @param currUnionOp
   *          current union operator
   */
  public void setCurrUnionOp(UnionOperator currUnionOp) {
    this.currUnionOp = currUnionOp;
  }

  /**
   * @return current top alias
   */
  public String getCurrAliasId() {
    return currAliasId;
  }

  /**
   * @param currAliasId
   *          current top alias
   */
  public void setCurrAliasId(String currAliasId) {
    this.currAliasId = currAliasId;
  }

  public GenMRUnionCtx getUnionTask(UnionOperator op) {
    return unionTaskMap.get(op);
  }

  public void setUnionTask(UnionOperator op, GenMRUnionCtx uTask) {
    unionTaskMap.put(op, uTask);
  }

  /**
   * Get the input set.
   */
  public Set<ReadEntity> getInputs() {
    return inputs;
  }

  /**
   * Get the output set.
   */
  public Set<WriteEntity> getOutputs() {
    return outputs;
  }

  /**
   * @return the conf
   */
  public HiveConf getConf() {
    return conf;
  }

  /**
   * @param conf
   *          the conf to set
   */
  public void setConf(HiveConf conf) {
    this.conf = conf;
  }

  /**
   * Returns dependencyTaskForMultiInsert initializing it if necessary.
   *
   * dependencyTaskForMultiInsert serves as a mutual dependency for the final move tasks in a
   * multi-insert query.
   *
   * @return
   */
  public DependencyCollectionTask getDependencyTaskForMultiInsert() {
    if (dependencyTaskForMultiInsert == null) {
      if (conf.getBoolVar(ConfVars.HIVE_MULTI_INSERT_MOVE_TASKS_SHARE_DEPENDENCIES)) {
        dependencyTaskForMultiInsert =
            (DependencyCollectionTask) TaskFactory.get(new DependencyCollectionWork(), conf);
      }
    }
    return dependencyTaskForMultiInsert;
  }

  public Map<FileSinkDesc, Task<? extends Serializable>> getLinkedFileDescTasks() {
    return linkedFileDescTasks;
  }

  public void setLinkedFileDescTasks(
      Map<FileSinkDesc, Task<? extends Serializable>> linkedFileDescTasks) {
    this.linkedFileDescTasks = linkedFileDescTasks;
  }
}
