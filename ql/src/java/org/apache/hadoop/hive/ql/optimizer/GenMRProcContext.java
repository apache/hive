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
import org.apache.hadoop.hive.ql.exec.DependencyCollectionTask;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
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
    Operator<? extends OperatorDesc> currTopOp;
    String currAliasId;

    public GenMapRedCtx() {
    }

    /**
     * @param currTask
     *          the current task
     * @param currTopOp
     *          the current top operator being traversed
     * @param currAliasId
     *          the current alias for the to operator
     */
    public GenMapRedCtx(Task<? extends Serializable> currTask,
        Operator<? extends OperatorDesc> currTopOp, String currAliasId) {
      this.currTask = currTask;
      this.currTopOp = currTopOp;
      this.currAliasId = currAliasId;
    }

    /**
     * @return current task
     */
    public Task<? extends Serializable> getCurrTask() {
      return currTask;
    }

    /**
     * @return current top operator
     */
    public Operator<? extends OperatorDesc> getCurrTopOp() {
      return currTopOp;
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
    Task<? extends Serializable> uTask;
    List<String> taskTmpDir;
    List<TableDesc> tt_desc;
    List<Operator<? extends OperatorDesc>> listTopOperators;

    public GenMRUnionCtx() {
      uTask = null;
      taskTmpDir = new ArrayList<String>();
      tt_desc = new ArrayList<TableDesc>();
      listTopOperators = new ArrayList<Operator<? extends OperatorDesc>>();
    }

    public Task<? extends Serializable> getUTask() {
      return uTask;
    }

    public void setUTask(Task<? extends Serializable> uTask) {
      this.uTask = uTask;
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

    public List<Operator<? extends OperatorDesc>> getListTopOperators() {
      return listTopOperators;
    }

    public void setListTopOperators(
        List<Operator<? extends OperatorDesc>> listTopOperators) {
      this.listTopOperators = listTopOperators;
    }

    public void addListTopOperators(Operator<? extends OperatorDesc> topOperator) {
      listTopOperators.add(topOperator);
    }
  }

  private HiveConf conf;
  private
    HashMap<Operator<? extends OperatorDesc>, Task<? extends Serializable>> opTaskMap;
  private HashMap<UnionOperator, GenMRUnionCtx> unionTaskMap;
  private List<Operator<? extends OperatorDesc>> seenOps;
  private List<FileSinkOperator> seenFileSinkOps;

  private ParseContext parseCtx;
  private List<Task<MoveWork>> mvTask;
  private List<Task<? extends Serializable>> rootTasks;

  private LinkedHashMap<Operator<? extends OperatorDesc>, GenMapRedCtx> mapCurrCtx;
  private Task<? extends Serializable> currTask;
  private Operator<? extends OperatorDesc> currTopOp;
  private UnionOperator currUnionOp;
  private String currAliasId;
  private List<Operator<? extends OperatorDesc>> rootOps;
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
      List<Operator<? extends OperatorDesc>> seenOps, ParseContext parseCtx,
      List<Task<MoveWork>> mvTask,
      List<Task<? extends Serializable>> rootTasks,
      LinkedHashMap<Operator<? extends OperatorDesc>, GenMapRedCtx> mapCurrCtx,
      Set<ReadEntity> inputs, Set<WriteEntity> outputs) {
    this.conf = conf;
    this.opTaskMap = opTaskMap;
    this.seenOps = seenOps;
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
    rootOps = new ArrayList<Operator<? extends OperatorDesc>>();
    rootOps.addAll(parseCtx.getTopOps().values());
    unionTaskMap = new HashMap<UnionOperator, GenMRUnionCtx>();
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

  /**
   * @return operators already visited
   */
  public List<Operator<? extends OperatorDesc>> getSeenOps() {
    return seenOps;
  }

  /**
   * @return file operators already visited
   */
  public List<FileSinkOperator> getSeenFileSinkOps() {
    return seenFileSinkOps;
  }

  /**
   * @param seenOps
   *          operators already visited
   */
  public void setSeenOps(List<Operator<? extends OperatorDesc>> seenOps) {
    this.seenOps = seenOps;
  }

  /**
   * @param seenFileSinkOps
   *          file sink operators already visited
   */
  public void setSeenFileSinkOps(List<FileSinkOperator> seenFileSinkOps) {
    this.seenFileSinkOps = seenFileSinkOps;
  }

  /**
   * @return top operators for tasks
   */
  public List<Operator<? extends OperatorDesc>> getRootOps() {
    return rootOps;
  }

  /**
   * @param rootOps
   *          top operators for tasks
   */
  public void setRootOps(List<Operator<? extends OperatorDesc>> rootOps) {
    this.rootOps = rootOps;
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
  public Operator<? extends OperatorDesc> getCurrTopOp() {
    return currTopOp;
  }

  /**
   * @param currTopOp
   *          current top operator
   */
  public void setCurrTopOp(Operator<? extends OperatorDesc> currTopOp) {
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
      dependencyTaskForMultiInsert =
          (DependencyCollectionTask) TaskFactory.get(new DependencyCollectionWork(), conf);
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
