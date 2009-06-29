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

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.io.Serializable;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.UnionOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.plan.tableDesc;

/**
 * Processor Context for creating map reduce task. Walk the tree in a DFS manner and process the nodes. Some state is 
 * maintained about the current nodes visited so far.
 */
public class GenMRProcContext implements NodeProcessorCtx {

  /** 
   * GenMapRedCtx is used to keep track of the current state. 
   */
  public static class GenMapRedCtx {
    Task<? extends Serializable>         currTask;
    Operator<? extends Serializable>     currTopOp;
    String                               currAliasId;
    
    /**
     * @param currTask    the current task
     * @param currTopOp   the current top operator being traversed
     * @param currAliasId the current alias for the to operator
     */
    public GenMapRedCtx (Task<? extends Serializable>         currTask,
                         Operator<? extends Serializable>     currTopOp,
                         String                               currAliasId) {
      this.currTask    = currTask;
      this.currTopOp   = currTopOp;
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
    public Operator<? extends Serializable> getCurrTopOp() {
      return currTopOp;
    }

    /**
     * @return current alias
     */
    public String getCurrAliasId() {
      return currAliasId;
    }
  }

  public static class GenMRUnionCtx {
    Task<? extends Serializable>         uTask;
    List<String>                         taskTmpDir;
    List<tableDesc>                      tt_desc; 

    public GenMRUnionCtx() { 
      uTask = null;
      taskTmpDir = new ArrayList<String>();
      tt_desc = new ArrayList<tableDesc>(); 
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

    public void addTTDesc(tableDesc tt_desc) {
      this.tt_desc.add(tt_desc);
    }

    public List<tableDesc> getTTDesc() {
      return tt_desc;
    }
  }

  public static class GenMRMapJoinCtx {
    String                            taskTmpDir;
    tableDesc                         tt_desc; 
    Operator<? extends Serializable>  rootMapJoinOp;
    MapJoinOperator                   oldMapJoin;   
    
    public GenMRMapJoinCtx() { 
      taskTmpDir    = null;
      tt_desc       = null;
      rootMapJoinOp = null;
      oldMapJoin    = null;
    }

    /**
     * @param taskTmpDir
     * @param tt_desc
     * @param childSelect
     * @param oldMapJoin
     */
    public GenMRMapJoinCtx(String taskTmpDir, tableDesc tt_desc, 
        Operator<? extends Serializable> rootMapJoinOp, MapJoinOperator oldMapJoin) {
      this.taskTmpDir    = taskTmpDir;
      this.tt_desc       = tt_desc;
      this.rootMapJoinOp = rootMapJoinOp;
      this.oldMapJoin    = oldMapJoin;
    }
    
    public void setTaskTmpDir(String taskTmpDir) {
      this.taskTmpDir = taskTmpDir;
    }

    public String getTaskTmpDir() {
      return taskTmpDir;
    }

    public void setTTDesc(tableDesc tt_desc) {
      this.tt_desc = tt_desc;
    }

    public tableDesc getTTDesc() {
      return tt_desc;
    }

    /**
     * @return the childSelect
     */
    public Operator<? extends Serializable> getRootMapJoinOp() {
      return rootMapJoinOp;
    }

    /**
     * @param rootMapJoinOp the rootMapJoinOp to set
     */
    public void setRootMapJoinOp(Operator<? extends Serializable> rootMapJoinOp) {
      this.rootMapJoinOp = rootMapJoinOp;
    }

    /**
     * @return the oldMapJoin
     */
    public MapJoinOperator getOldMapJoin() {
      return oldMapJoin;
    }

    /**
     * @param oldMapJoin the oldMapJoin to set
     */
    public void setOldMapJoin(MapJoinOperator oldMapJoin) {
      this.oldMapJoin = oldMapJoin;
    }
  }

  private HiveConf conf;
  private HashMap<Operator<? extends Serializable>, Task<? extends Serializable>> opTaskMap;
  private HashMap<UnionOperator, GenMRUnionCtx>      unionTaskMap;
  private HashMap<MapJoinOperator, GenMRMapJoinCtx>  mapJoinTaskMap;
  private List<Operator<? extends Serializable>> seenOps;
  private List<FileSinkOperator>                 seenFileSinkOps;

  private ParseContext                          parseCtx;
  private List<Task<? extends Serializable>>    mvTask;
  private List<Task<? extends Serializable>>    rootTasks;

  private Map<Operator<? extends Serializable>, GenMapRedCtx> mapCurrCtx; 
  private Task<? extends Serializable>         currTask;
  private Operator<? extends Serializable>     currTopOp;
  private UnionOperator                        currUnionOp;
  private MapJoinOperator                      currMapJoinOp;
  private String                               currAliasId;
  private List<Operator<? extends Serializable>> rootOps;
  
  /**
   * Set of read entities. This list is generated by the walker and is 
   * passed to the hooks.
   */
  private Set<ReadEntity>                     inputs;
  /**
   * Set of write entities. This list is generated by the walker and is
   * passed to the hooks.
   */
  private Set<WriteEntity>                    outputs;
  
  /**
   * @param conf       hive configuration
   * @param opTaskMap  reducer to task mapping
   * @param seenOps    operator already visited
   * @param parseCtx   current parse context
   * @param rootTasks  root tasks for the plan
   * @param mvTask     the final move task
   * @param mapCurrCtx operator to task mappings
   * @param inputs     the set of input tables/partitions generated by the walk
   * @param outputs    the set of destinations generated by the walk
   */
  public GenMRProcContext (
    HiveConf conf,
    HashMap<Operator<? extends Serializable>, Task<? extends Serializable>> opTaskMap,
    List<Operator<? extends Serializable>> seenOps,
    ParseContext                           parseCtx,
    List<Task<? extends Serializable>>     mvTask,
    List<Task<? extends Serializable>>     rootTasks,
    Map<Operator<? extends Serializable>, GenMapRedCtx> mapCurrCtx,
    Set<ReadEntity> inputs,
    Set<WriteEntity> outputs) 
  {
    this.conf       = conf;
    this.opTaskMap  = opTaskMap;
    this.seenOps    = seenOps;
    this.mvTask     = mvTask;
    this.parseCtx   = parseCtx;
    this.rootTasks  = rootTasks;
    this.mapCurrCtx = mapCurrCtx;
    this.inputs = inputs;
    this.outputs = outputs;
    currTask        = null;
    currTopOp       = null;
    currUnionOp     = null;
    currMapJoinOp   = null;
    currAliasId     = null;
    rootOps         = new ArrayList<Operator<? extends Serializable>>();
    rootOps.addAll(parseCtx.getTopOps().values());
    unionTaskMap = new HashMap<UnionOperator, GenMRUnionCtx>();
    mapJoinTaskMap = new HashMap<MapJoinOperator, GenMRMapJoinCtx>();
  }

  /**
   * @return  reducer to task mapping
   */
  public HashMap<Operator<? extends Serializable>, Task<? extends Serializable>> getOpTaskMap() {
    return opTaskMap;
  }

  /**
   * @param opTaskMap  reducer to task mapping
   */
  public void setOpTaskMap(HashMap<Operator<? extends Serializable>, Task<? extends Serializable>> opTaskMap) {
    this.opTaskMap = opTaskMap;
  }

  /**
   * @return  operators already visited
   */
  public List<Operator<? extends Serializable>> getSeenOps() {
    return seenOps;
  }

  /**
   * @return  file operators already visited
   */
  public List<FileSinkOperator> getSeenFileSinkOps() {
    return seenFileSinkOps;
  }

  /**
   * @param seenOps    operators already visited
   */
  public void setSeenOps(List<Operator<? extends Serializable>> seenOps) {
    this.seenOps = seenOps;
  }

  /**
   * @param seenFileSinkOps file sink operators already visited
   */
  public void setSeenFileSinkOps(List<FileSinkOperator> seenFileSinkOps) {
    this.seenFileSinkOps = seenFileSinkOps;
  }

  /**
   * @return  top operators for tasks
   */
  public List<Operator<? extends Serializable>> getRootOps() {
    return rootOps;
  }

  /**
   * @param rootOps    top operators for tasks
   */
  public void setRootOps(List<Operator<? extends Serializable>> rootOps) {
    this.rootOps = rootOps;
  }

  /**
   * @return   current parse context
   */
  public ParseContext getParseCtx() {
    return parseCtx;
  }

  /**
   * @param parseCtx   current parse context
   */
  public void setParseCtx(ParseContext parseCtx) {
    this.parseCtx = parseCtx;
  }

  /**
   * @return     the final move task
   */
  public List<Task<? extends Serializable>> getMvTask() {
    return mvTask;
  }

  /**
   * @param mvTask     the final move task
   */
  public void setMvTask(List<Task<? extends Serializable>> mvTask) {
    this.mvTask = mvTask;
  }

  /**
   * @return  root tasks for the plan
   */
  public List<Task<? extends Serializable>>  getRootTasks() {
    return rootTasks;
  }

  /**
   * @param rootTasks  root tasks for the plan
   */
  public void setRootTasks(List<Task<? extends Serializable>>  rootTasks) {
    this.rootTasks = rootTasks;
  }

  /**
   * @return operator to task mappings
   */
  public Map<Operator<? extends Serializable>, GenMapRedCtx> getMapCurrCtx() {
    return mapCurrCtx;
  }

  /**
   * @param mapCurrCtx operator to task mappings
   */
  public void setMapCurrCtx(Map<Operator<? extends Serializable>, GenMapRedCtx> mapCurrCtx) {
    this.mapCurrCtx = mapCurrCtx;
  }

  /**
   * @return current task
   */
  public Task<? extends Serializable>  getCurrTask() {
    return currTask;
  }

  /**
   * @param currTask current task
   */
  public void setCurrTask(Task<? extends Serializable>  currTask) {
    this.currTask = currTask;
  }

  /**
   * @return current top operator
   */
  public Operator<? extends Serializable> getCurrTopOp() {
    return currTopOp;
  }   
   
  /**
   * @param currTopOp current top operator
   */
  public void setCurrTopOp(Operator<? extends Serializable> currTopOp) {
    this.currTopOp = currTopOp;
  }      

  public UnionOperator getCurrUnionOp() {
    return currUnionOp;
  }   
   
  /**
   * @param currUnionOp current union operator
   */
  public void setCurrUnionOp(UnionOperator currUnionOp) {
    this.currUnionOp = currUnionOp;
  }      

  public MapJoinOperator getCurrMapJoinOp() {
    return currMapJoinOp;
  }   
   
  /**
   * @param currMapJoinOp current map join operator
   */
  public void setCurrMapJoinOp(MapJoinOperator currMapJoinOp) {
    this.currMapJoinOp = currMapJoinOp;
  }      

  /**
   * @return current top alias
   */
  public String  getCurrAliasId() {
    return currAliasId;
  }

  /**
   * @param currAliasId current top alias
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

  public GenMRMapJoinCtx getMapJoinCtx(MapJoinOperator op) {
    return mapJoinTaskMap.get(op);
  }

  public void setMapJoinCtx(MapJoinOperator op, GenMRMapJoinCtx mjCtx) {
    mapJoinTaskMap.put(op, mjCtx);
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
   * @param conf the conf to set
   */
  public void setConf(HiveConf conf) {
    this.conf = conf;
  }
}
