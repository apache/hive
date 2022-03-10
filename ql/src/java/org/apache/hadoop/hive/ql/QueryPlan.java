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

package org.apache.hadoop.hive.ql;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.ql.ddl.DDLDesc.DDLDescWithWriteId;
import org.apache.hadoop.hive.ql.exec.ConditionalTask;
import org.apache.hadoop.hive.ql.exec.ExplainTask;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.mr.ExecDriver;
import org.apache.hadoop.hive.ql.hooks.LineageInfo;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.ColumnAccessInfo;
import org.apache.hadoop.hive.ql.parse.TableAccessInfo;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.ReducerTimeStatsPerJob;
import org.apache.hadoop.hive.ql.plan.api.AdjacencyType;
import org.apache.hadoop.hive.ql.plan.api.NodeType;
import org.apache.hadoop.hive.ql.plan.api.TaskType;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.transport.TMemoryBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * QueryPlan can be serialized to disk so that we can restart/resume the
 * progress of it in the future, either within or outside of the current
 * jvm.
 */
public class QueryPlan implements Serializable {
  private static final long serialVersionUID = 1L;

  private static final Logger LOG = LoggerFactory.getLogger(QueryPlan.class);

  private String cboInfo;
  private String queryString;
  private String optimizedCBOPlan;
  private String optimizedQueryString;

  private List<Task<?>> rootTasks;
  private FetchTask fetchTask;
  private final List<ReducerTimeStatsPerJob> reducerTimeStatsPerJobList;

  private Set<ReadEntity> inputs;
  /**
   * Note: outputs are not all determined at compile time.
   * Some of the tasks can change the outputs at run time, because only at run
   * time, we know what are the changes.  These tasks should keep a reference
   * to the outputs here.
   */
  private Set<WriteEntity> outputs;
  /**
   * Lineage information for the query.
   */
  protected LineageInfo linfo;
  private TableAccessInfo tableAccessInfo;
  private ColumnAccessInfo columnAccessInfo;
  private Schema resultSchema;

  private Map<String, String> idToTableNameMap;

  private String queryId;
  private org.apache.hadoop.hive.ql.plan.api.Query query;
  private final Map<String, Map<String, Long>> counters =
      new ConcurrentHashMap<String, Map<String, Long>>();
  private final Set<String> done = Collections.newSetFromMap(new
      ConcurrentHashMap<String, Boolean>());
  private final Set<String> started = Collections.newSetFromMap(new
      ConcurrentHashMap<String, Boolean>());

  private QueryProperties queryProperties;

  private transient Long queryStartTime;
  private final HiveOperation operation;
  private final boolean acidResourcesInQuery;
  private final Set<FileSinkDesc> acidSinks; // Note: both full-ACID and insert-only sinks.
  private final WriteEntity acidAnalyzeTable;
  private final DDLDescWithWriteId acidDdlDesc;
  private Boolean autoCommitValue;

  private Boolean prepareQuery;

  public QueryPlan() {
    this(null);
  }
  @VisibleForTesting
  protected QueryPlan(HiveOperation command) {
    this.reducerTimeStatsPerJobList = new ArrayList<>();
    this.operation = command;
    this.acidResourcesInQuery = false;
    this.acidSinks = Collections.emptySet();
    this.acidDdlDesc = null;
    this.acidAnalyzeTable = null;
    this.prepareQuery = false;
  }

  public QueryPlan(String queryString, BaseSemanticAnalyzer sem, Long startTime, String queryId,
                  HiveOperation operation, Schema resultSchema) {
    this.queryString = queryString;

    rootTasks = new ArrayList<Task<?>>(sem.getAllRootTasks());
    reducerTimeStatsPerJobList = new ArrayList<ReducerTimeStatsPerJob>();
    fetchTask = sem.getFetchTask();
    // Note that inputs and outputs can be changed when the query gets executed
    inputs = sem.getAllInputs();
    outputs = sem.getAllOutputs();
    linfo = sem.getLineageInfo();
    tableAccessInfo = sem.getTableAccessInfo();
    columnAccessInfo = sem.getColumnAccessInfo();
    idToTableNameMap = new HashMap<String, String>(sem.getIdToTableNameMap());

    this.queryId = queryId == null ? makeQueryId() : queryId;
    query = new org.apache.hadoop.hive.ql.plan.api.Query();
    query.setQueryId(this.queryId);
    query.putToQueryAttributes("queryString", this.queryString);
    queryProperties = sem.getQueryProperties();
    queryStartTime = startTime;
    this.operation = operation;
    this.autoCommitValue = sem.getAutoCommitValue();
    this.resultSchema = resultSchema;
    // TODO: all this ACID stuff should be in some sub-object
    this.acidResourcesInQuery = sem.hasTransactionalInQuery();
    this.acidSinks = sem.getAcidFileSinks();
    this.acidDdlDesc = sem.getAcidDdlDesc();
    this.acidAnalyzeTable = sem.getAcidAnalyzeTable();
    this.cboInfo = sem.getCboInfo();
    this.prepareQuery = false;
  }

  /**
   * @return true if any acid resources are read/written
   */
  public boolean hasAcidResourcesInQuery() {
    return acidResourcesInQuery;
  }

  public WriteEntity getAcidAnalyzeTable() {
    return acidAnalyzeTable;
  }

  /**
   * @return Collection of FileSinkDesc representing writes to Acid resources
   */
  Set<FileSinkDesc> getAcidSinks() {
    return acidSinks;
  }

  /**
   * This method is to get the proper statementId for the FileSinkOperator, a particular MoveTask belongs to.
   * This is needed for ACID operations with direct insert on. In this case for listing the newly added or modified
   * data the MoveTask has to know the statementId in order to list the files from the proper folder.
   * Without knowing the statementId the files could be listed by multiple MoveTasks which could cause issues.
   * To get the statementId, first the FSO has to be found in the acidSinks list. To do that, use
   * the ACID operation, the path, the writeId and the moveTaskId.
   *
   * For queries with union all optimisation, there will be multiple FSOs with the same operation, writeId and moveTaskId.
   * But one of these FSOs doesn't write data and its statementId is not valid, so if this FSO is selected and its statementId
   * is returned, the file listing will find nothing. So check the acidSinks and if two of them have the same writeId, path
   * and moveTaskId, then return -1 as statementId. With doing this, the file listing will find all partitions and files correctly.
   *
   * @param writeId
   * @param moveTaskId
   * @param acidOperation
   * @param path
   * @return The statementId from the FileSinkOperator with the given writeId, moveTaskId, operation and path.
   * -1 if there are multiple FileSinkOperators with the same value of these parameters.
   * The original statement id if there were no matching acid sinks.
   */
  public Integer getStatementIdForAcidWriteType(long writeId, String moveTaskId, AcidUtils.Operation acidOperation, Path path,
                                                int originalStatementId) {
    FileSinkDesc result = null;
    for (FileSinkDesc acidSink : acidSinks) {
      if (acidOperation.equals(acidSink.getAcidOperation()) && path.equals(acidSink.getDestPath())
          && acidSink.getTableWriteId() == writeId
          && (moveTaskId == null || acidSink.getMoveTaskId() == null || moveTaskId.equals(acidSink.getMoveTaskId()))) {
        if (result != null) {
          return -1;
        }
        result = acidSink;
      }
    }
    if (result != null) {
      return result.getStatementId();
    } else {
      // If there were no matching acid sinks proceed with the original statement id. This can happen, if we used the
      // load data inpath command on an insert only table.
      return originalStatementId;
    }
  }

  /**
   * This method is to get the dynamic partition specifications inserted by the FileSinkOperator, a particular MoveTask belongs to.
   * This is needed for insert overwrite queries for ACID tables with direct insert on, so each MoveTask could list only the
   * files inserted by the FSO the MoveTask belongs to. In case of an insert query, the writeId and statementId is enough to
   * identify which delta directory was written by which FSO. But in case of insert overwrite, base directories will be created
   * without statementIds, so we need the partition information to identify which folders to list.
   *
   * For queries with union all optimisation, there will be multiple FSOs with the same operation, writeId and moveTaskId.
   * But one of these FSOs doesn't contain the partition specifications, so if this FSO is selected, the file listing will not be correct.
   * So check the acidSinks and if two of them have the same writeId, path and moveTaskId, then return null.
   * With doing this, the file listing will find all partitions and files correctly.
   *
   * @param writeId
   * @param moveTaskId
   * @param acidOperation
   * @param path
   * @return The dynamic partition specifications from the FileSinkOperator with the given writeId, moveTaskId, operation and path.
   * null if there are multiple FileSinkOperators with the same value of these parameters.
   */
  public Map<String, List<Path>> getDynamicPartitionSpecs(long writeId, String moveTaskId, AcidUtils.Operation acidOperation, Path path) {
    FileSinkDesc result = null;
    for (FileSinkDesc acidSink : acidSinks) {
      if (acidOperation.equals(acidSink.getAcidOperation()) && path.equals(acidSink.getDestPath())
          && acidSink.getTableWriteId() == writeId
          && (moveTaskId == null || acidSink.getMoveTaskId() == null || moveTaskId.equals(acidSink.getMoveTaskId()))) {
        if (result != null) {
          return null;
        }
        result = acidSink;
      }
    }
    if (result != null) {
      return result.getDynPartitionValues();
    } else {
      return null;
    }
  }

  DDLDescWithWriteId getAcidDdlDesc() {
    return acidDdlDesc;
  }

  public String getQueryStr() {
    return queryString;
  }

  public String getQueryId() {
    return queryId;
  }

  public void setPrepareQuery(boolean prepareQuery) {
    this.prepareQuery = prepareQuery;
  }

  public boolean isPrepareQuery() {
    return prepareQuery;
  }

  public static String makeQueryId() {
    GregorianCalendar gc = new GregorianCalendar();
    String userid = System.getProperty("user.name");

    return userid
        + "_"
        + String.format("%1$4d%2$02d%3$02d%4$02d%5$02d%6$02d", gc
        .get(Calendar.YEAR), gc.get(Calendar.MONTH) + 1, gc
        .get(Calendar.DAY_OF_MONTH), gc.get(Calendar.HOUR_OF_DAY), gc
        .get(Calendar.MINUTE), gc.get(Calendar.SECOND))
        + "_"
        + UUID.randomUUID().toString();
  }

  /**
   * generate the operator graph and operator list for the given task based on
   * the operators corresponding to that task.
   *
   * @param task
   *          api.Task which needs its operator graph populated
   * @param topOps
   *          the set of top operators from which the operator graph for the
   *          task is hanging
   */
  private void populateOperatorGraph(
      org.apache.hadoop.hive.ql.plan.api.Task task,
      Collection<Operator<? extends OperatorDesc>> topOps) {

    task.setOperatorGraph(new org.apache.hadoop.hive.ql.plan.api.Graph());
    task.getOperatorGraph().setNodeType(NodeType.OPERATOR);

    Queue<Operator<? extends OperatorDesc>> opsToVisit =
      new LinkedList<Operator<? extends OperatorDesc>>();
    Set<Operator<? extends OperatorDesc>> opsVisited =
      new HashSet<Operator<? extends OperatorDesc>>();
    opsToVisit.addAll(topOps);
    while (opsToVisit.peek() != null) {
      Operator<? extends OperatorDesc> op = opsToVisit.remove();
      opsVisited.add(op);
      // populate the operator
      org.apache.hadoop.hive.ql.plan.api.Operator operator =
        new org.apache.hadoop.hive.ql.plan.api.Operator();
      operator.setOperatorId(op.getOperatorId());
      operator.setOperatorType(op.getType());
      task.addToOperatorList(operator);
      // done processing the operator
      if (op.getChildOperators() != null) {
        org.apache.hadoop.hive.ql.plan.api.Adjacency entry =
          new org.apache.hadoop.hive.ql.plan.api.Adjacency();
        entry.setAdjacencyType(AdjacencyType.CONJUNCTIVE);
        entry.setNode(op.getOperatorId());
        for (Operator<? extends OperatorDesc> childOp : op.getChildOperators()) {
          entry.addToChildren(childOp.getOperatorId());
          if (!opsVisited.contains(childOp)) {
            opsToVisit.add(childOp);
          }
        }
        task.getOperatorGraph().addToAdjacencyList(entry);
      }
    }
  }

  /**
   * Populate api.QueryPlan from exec structures. This includes constructing the
   * dependency graphs of stages and operators.
   *
   * @throws IOException
   */
  private void populateQueryPlan() throws IOException {
    query.setStageGraph(new org.apache.hadoop.hive.ql.plan.api.Graph());
    query.getStageGraph().setNodeType(NodeType.STAGE);

    Queue<Task<?>> tasksToVisit =
      new LinkedList<Task<?>>();
    Set<Task<?>> tasksVisited = new HashSet<Task<?>>();
    tasksToVisit.addAll(rootTasks);
    while (tasksToVisit.size() != 0) {
      Task<?> task = tasksToVisit.remove();
      tasksVisited.add(task);
      // populate stage
      org.apache.hadoop.hive.ql.plan.api.Stage stage =
        new org.apache.hadoop.hive.ql.plan.api.Stage();
      stage.setStageId(task.getId());
      stage.setStageType(task.getType());
      query.addToStageList(stage);

      if (task instanceof ExecDriver) {
        // populate map task
        ExecDriver mrTask = (ExecDriver) task;
        org.apache.hadoop.hive.ql.plan.api.Task mapTask =
          new org.apache.hadoop.hive.ql.plan.api.Task();
        mapTask.setTaskId(stage.getStageId() + "_MAP");
        mapTask.setTaskType(TaskType.MAP);
        stage.addToTaskList(mapTask);
        populateOperatorGraph(mapTask, mrTask.getWork().getMapWork().getAliasToWork()
            .values());

        // populate reduce task
        if (mrTask.hasReduce()) {
          org.apache.hadoop.hive.ql.plan.api.Task reduceTask =
            new org.apache.hadoop.hive.ql.plan.api.Task();
          reduceTask.setTaskId(stage.getStageId() + "_REDUCE");
          reduceTask.setTaskType(TaskType.REDUCE);
          stage.addToTaskList(reduceTask);
          Collection<Operator<? extends OperatorDesc>> reducerTopOps =
            new ArrayList<Operator<? extends OperatorDesc>>();
          reducerTopOps.add(mrTask.getWork().getReduceWork().getReducer());
          populateOperatorGraph(reduceTask, reducerTopOps);
        }
      } else {
        org.apache.hadoop.hive.ql.plan.api.Task otherTask =
          new org.apache.hadoop.hive.ql.plan.api.Task();
        otherTask.setTaskId(stage.getStageId() + "_OTHER");
        otherTask.setTaskType(TaskType.OTHER);
        stage.addToTaskList(otherTask);
      }
      if (task instanceof ConditionalTask) {
        org.apache.hadoop.hive.ql.plan.api.Adjacency listEntry =
          new org.apache.hadoop.hive.ql.plan.api.Adjacency();
        listEntry.setAdjacencyType(AdjacencyType.DISJUNCTIVE);
        listEntry.setNode(task.getId());
        ConditionalTask t = (ConditionalTask) task;

        for (Task<?> listTask : t.getListTasks()) {
          if (t.getChildTasks() != null) {
            org.apache.hadoop.hive.ql.plan.api.Adjacency childEntry =
              new org.apache.hadoop.hive.ql.plan.api.Adjacency();
            childEntry.setAdjacencyType(AdjacencyType.DISJUNCTIVE);
            childEntry.setNode(listTask.getId());
            // done processing the task
            for (Task<?> childTask : t.getChildTasks()) {
              childEntry.addToChildren(childTask.getId());
              if (!tasksVisited.contains(childTask)) {
                tasksToVisit.add(childTask);
              }
            }
            query.getStageGraph().addToAdjacencyList(childEntry);
          }

          listEntry.addToChildren(listTask.getId());
          if (!tasksVisited.contains(listTask)) {
            tasksToVisit.add(listTask);
          }
        }
        query.getStageGraph().addToAdjacencyList(listEntry);
      } else if (task.getChildTasks() != null) {
        org.apache.hadoop.hive.ql.plan.api.Adjacency entry =
          new org.apache.hadoop.hive.ql.plan.api.Adjacency();
        entry.setAdjacencyType(AdjacencyType.CONJUNCTIVE);
        entry.setNode(task.getId());
        // done processing the task
        for (Task<?> childTask : task.getChildTasks()) {
          entry.addToChildren(childTask.getId());
          if (!tasksVisited.contains(childTask)) {
            tasksToVisit.add(childTask);
          }
        }
        query.getStageGraph().addToAdjacencyList(entry);
      }
    }
  }

  /**
   * From the counters extracted via extractCounters(), update the counters in
   * the query plan.
   */
  private void updateCountersInQueryPlan() {
    query.setStarted(started.contains(query.getQueryId()));
    query.setDone(done.contains(query.getQueryId()));
    if (query.getStageList() != null) {
      for (org.apache.hadoop.hive.ql.plan.api.Stage stage : query
          .getStageList()) {
        if (stage.getStageId() == null) {
          continue;
        }
        stage.setStarted(started.contains(stage.getStageId()));
        stage.setStageCounters(counters.get(stage.getStageId()));
        stage.setDone(done.contains(stage.getStageId()));
        for (org.apache.hadoop.hive.ql.plan.api.Task task : stage.getTaskList()) {
          task.setTaskCounters(counters.get(task.getTaskId()));
          if (task.getTaskType() == TaskType.OTHER) {
            task.setStarted(started.contains(stage.getStageId()));
            task.setDone(done.contains(stage.getStageId()));
          } else {
            task.setStarted(started.contains(task.getTaskId()));
            task.setDone(done.contains(task.getTaskId()));
            if (task.getOperatorList() == null) {
              return;
            }
            for (org.apache.hadoop.hive.ql.plan.api.Operator op :
              task.getOperatorList()) {
              // if the task has started, all operators within the task have
              // started
              op.setStarted(started.contains(task.getTaskId()));
              // if the task is done, all operators are done as well
              op.setDone(done.contains(task.getTaskId()));
            }
          }
        }
      }
    }
  }

  /**
   * Extract all the counters from tasks and operators.
   */
  private void extractCounters() throws IOException {
    Queue<Task<?>> tasksToVisit =
      new LinkedList<Task<?>>();
    Set<Task<?>> tasksVisited =
      new HashSet<Task<?>>();
    tasksToVisit.addAll(rootTasks);
    while (tasksToVisit.peek() != null) {
      Task<?> task = tasksToVisit.remove();
      tasksVisited.add(task);
      // add children to tasksToVisit
      if (task.getChildTasks() != null) {
        for (Task<?> childTask : task.getChildTasks()) {
          if (!tasksVisited.contains(childTask)) {
            tasksToVisit.add(childTask);
          }
        }
      }
      if (task.getId() == null) {
        continue;
      }
      if (started.contains(task.getId()) && done.contains(task.getId())) {
        continue;
      }

      // get the counters for the task
      counters.put(task.getId(), task.getCounters());

      // check if task is started
      if (task.started()) {
        started.add(task.getId());
      }
      if (task.done()) {
        done.add(task.getId());
      }
      if (task instanceof ExecDriver) {
        ExecDriver mrTask = (ExecDriver) task;
        if (mrTask.mapStarted()) {
          started.add(task.getId() + "_MAP");
        }
        if (mrTask.mapDone()) {
          done.add(task.getId() + "_MAP");
        }
        if (mrTask.hasReduce()) {
          if (mrTask.reduceStarted()) {
            started.add(task.getId() + "_REDUCE");
          }
          if (mrTask.reduceDone()) {
            done.add(task.getId() + "_REDUCE");
          }
        }
      } else if (task instanceof ConditionalTask) {
        ConditionalTask cTask = (ConditionalTask) task;
        for (Task<?> listTask : cTask.getListTasks()) {
          if (!tasksVisited.contains(listTask)) {
            tasksToVisit.add(listTask);
          }
        }
      }
    }
  }

  public org.apache.hadoop.hive.ql.plan.api.Query getQueryPlan()
      throws IOException {
    if (query.getStageGraph() == null) {
      populateQueryPlan();
    }
    extractCounters();
    updateCountersInQueryPlan();
    return query;
  }

  public String getJSONValue(Object value) {
    String v = "null";
    if (value != null) {
      v = value.toString();
      if (v.charAt(0) != '[' && v.charAt(0) != '{') {
        v = "\"" + v + "\"";
      }
    }
    return v;
  }

  public String getJSONKeyValue(Object key, Object value) {
    return "\"" + key + "\":" + getJSONValue(value) + ",";
  }

  @SuppressWarnings("rawtypes")
  private String getJSONList(List list) {
    if (list == null) {
      return "null";
    }
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    for (Object entry : list) {
      sb.append(getJSONValue(entry));
      sb.append(",");
    }
    sb.deleteCharAt(sb.length() - 1);
    sb.append("]");
    return sb.toString();
  }

  @SuppressWarnings("rawtypes")
  public String getJSONMap(Map map) {
    if (map == null) {
      return "null";
    }
    StringBuilder sb = new StringBuilder();
    sb.append("{");
    for (Object entry : map.entrySet()) {
      Map.Entry e = (Map.Entry) entry;
      sb.append(getJSONKeyValue(e.getKey(), e.getValue()));
    }
    sb.deleteCharAt(sb.length() - 1);
    sb.append("}");
    return sb.toString();
  }

  private Object getJSONGraph(org.apache.hadoop.hive.ql.plan.api.Graph graph) {
    if (graph == null) {
      return "null";
    }
    StringBuilder sb = new StringBuilder();
    sb.append("{");
    sb.append(getJSONKeyValue("nodeType", graph.getNodeType()));
    sb.append(getJSONKeyValue("roots", getJSONList(graph.getRoots())));
    // adjacency list
    List<String> adjList = new ArrayList<String>();
    if (graph.getAdjacencyList() != null) {
      for (org.apache.hadoop.hive.ql.plan.api.Adjacency adj : graph
          .getAdjacencyList()) {
        adjList.add(getJSONAdjacency(adj));
      }
    }
    sb.append(getJSONKeyValue("adjacencyList", getJSONList(adjList)));
    sb.deleteCharAt(sb.length() - 1);
    sb.append("}");
    return sb.toString();
  }

  private String getJSONAdjacency(
      org.apache.hadoop.hive.ql.plan.api.Adjacency adj) {
    if (adj == null) {
      return "null";
    }
    StringBuilder sb = new StringBuilder();
    sb.append("{");
    sb.append(getJSONKeyValue("node", adj.getNode()));
    sb.append(getJSONKeyValue("children", getJSONList(adj.getChildren())));
    sb.append(getJSONKeyValue("adjacencyType", adj.getAdjacencyType()));
    sb.deleteCharAt(sb.length() - 1);
    sb.append("}");
    return sb.toString();
  }

  private String getJSONOperator(org.apache.hadoop.hive.ql.plan.api.Operator op) {
    StringBuilder sb = new StringBuilder();
    sb.append("{");
    sb.append(getJSONKeyValue("operatorId", op.getOperatorId()));
    sb.append(getJSONKeyValue("operatorType", op.getOperatorType()));
    sb.append(getJSONKeyValue("operatorAttributes", getJSONMap(op.getOperatorAttributes())));
    sb.append(getJSONKeyValue("operatorCounters", getJSONMap(op.getOperatorCounters())));
    sb.append(getJSONKeyValue("done", op.isDone()));
    sb.append(getJSONKeyValue("started", op.isStarted()));
    sb.deleteCharAt(sb.length() - 1);
    sb.append("}");
    return sb.toString();
  }

  private String getJSONTask(org.apache.hadoop.hive.ql.plan.api.Task task) {
    StringBuilder sb = new StringBuilder();
    sb.append("{");
    sb.append(getJSONKeyValue("taskId", task.getTaskId()));
    sb.append(getJSONKeyValue("taskType", task.getTaskType()));
    sb.append(getJSONKeyValue("taskAttributes", getJSONMap(task.getTaskAttributes())));
    sb.append(getJSONKeyValue("taskCounters", getJSONMap(task.getTaskCounters())));
    sb.append(getJSONKeyValue("operatorGraph", getJSONGraph(task.getOperatorGraph())));
    // operator list
    List<String> opList = new ArrayList<String>();
    if (task.getOperatorList() != null) {
      for (org.apache.hadoop.hive.ql.plan.api.Operator op : task.getOperatorList()) {
        opList.add(getJSONOperator(op));
      }
    }
    sb.append(getJSONKeyValue("operatorList", getJSONList(opList)));
    sb.append(getJSONKeyValue("done", task.isDone()));
    sb.append(getJSONKeyValue("started", task.isStarted()));
    sb.deleteCharAt(sb.length() - 1);
    sb.append("}");
    return sb.toString();
  }

  private String getJSONStage(org.apache.hadoop.hive.ql.plan.api.Stage stage) {
    StringBuilder sb = new StringBuilder();
    sb.append("{");
    sb.append(getJSONKeyValue("stageId", stage.getStageId()));
    sb.append(getJSONKeyValue("stageType", stage.getStageType()));
    sb.append(getJSONKeyValue("stageAttributes", getJSONMap(stage.getStageAttributes())));
    sb.append(getJSONKeyValue("stageCounters", getJSONMap(stage.getStageCounters())));
    List<String> taskList = new ArrayList<String>();
    if (stage.getTaskList() != null) {
      for (org.apache.hadoop.hive.ql.plan.api.Task task : stage.getTaskList()) {
        taskList.add(getJSONTask(task));
      }
    }
    sb.append(getJSONKeyValue("taskList", getJSONList(taskList)));
    sb.append(getJSONKeyValue("done", stage.isDone()));
    sb.append(getJSONKeyValue("started", stage.isStarted()));
    sb.deleteCharAt(sb.length() - 1);
    sb.append("}");
    return sb.toString();
  }

  public String getJSONQuery(org.apache.hadoop.hive.ql.plan.api.Query query) {
    StringBuilder sb = new StringBuilder();
    sb.append("{");
    sb.append(getJSONKeyValue("queryId", query.getQueryId()));
    sb.append(getJSONKeyValue("queryType", query.getQueryType()));
    sb.append(getJSONKeyValue("queryAttributes", getJSONMap(query.getQueryAttributes())));
    sb.append(getJSONKeyValue("queryCounters", getJSONMap(query.getQueryCounters())));
    sb.append(getJSONKeyValue("stageGraph", getJSONGraph(query.getStageGraph())));
    // stageList
    List<String> stageList = new ArrayList<String>();
    if (query.getStageList() != null) {
      for (org.apache.hadoop.hive.ql.plan.api.Stage stage : query.getStageList()) {
        stageList.add(getJSONStage(stage));
      }
    }
    sb.append(getJSONKeyValue("stageList", getJSONList(stageList)));
    sb.append(getJSONKeyValue("done", query.isDone()));
    sb.append(getJSONKeyValue("started", query.isStarted()));
    sb.deleteCharAt(sb.length() - 1);
    sb.append("}");
    return sb.toString();
  }

  public boolean isExplain() {
    return rootTasks.size() == 1 && rootTasks.get(0) instanceof ExplainTask;
  }

  @Override
  public String toString() {
    try {
      return getJSONQuery(getQueryPlan());
    } catch (Exception e) {
      LOG.warn("Unable to produce query plan JSON string", e);
      return e.toString();
    }
  }

  public String toThriftJSONString() throws IOException {
    org.apache.hadoop.hive.ql.plan.api.Query q = getQueryPlan();
    try {
      TMemoryBuffer tmb = new TMemoryBuffer(q.toString().length() * 5);
      TJSONProtocol oprot = new TJSONProtocol(tmb);
      q.write(oprot);
      return tmb.toString(StandardCharsets.UTF_8);
    } catch (TException e) {
      LOG.warn("Unable to produce query plan Thrift string", e);
      return q.toString();
    }

  }

  public String toBinaryString() throws IOException {
    org.apache.hadoop.hive.ql.plan.api.Query q = getQueryPlan();
    try {
      TMemoryBuffer tmb = new TMemoryBuffer(q.toString().length() * 5);
      TBinaryProtocol oprot = new TBinaryProtocol(tmb);
      q.write(oprot);
      byte[] buf = new byte[tmb.length()];
      tmb.read(buf, 0, tmb.length());
      return new String(buf);
    } catch (TException e) {
      LOG.warn("Unable to produce query plan binary string", e);
      return q.toString();
    }
  }

  public void setStarted() {
    started.add(queryId);
  }

  public void setDone() {
    done.add(queryId);
  }

  public Set<String> getStarted() {
    return started;
  }

  public Set<String> getDone() {
    return done;
  }

  public List<Task<?>> getRootTasks() {
    return rootTasks;
  }

  public void setRootTasks(List<Task<?>> rootTasks) {
    this.rootTasks = rootTasks;
  }

  public boolean isForExplain() {
    return rootTasks.size() == 1 && rootTasks.get(0) instanceof ExplainTask;
  }

  public FetchTask getFetchTask() {
    return fetchTask;
  }

  public void setFetchTask(FetchTask fetchTask) {
    this.fetchTask = fetchTask;
  }

  public Set<ReadEntity> getInputs() {
    return inputs;
  }

  public void setInputs(HashSet<ReadEntity> inputs) {
    this.inputs = inputs;
  }

  public Set<WriteEntity> getOutputs() {
    return outputs;
  }

  public void setOutputs(HashSet<WriteEntity> outputs) {
    this.outputs = outputs;
  }

  public Schema getResultSchema() {
    return resultSchema;
  }

  public Map<String, String> getIdToTableNameMap() {
    return idToTableNameMap;
  }

  public void setIdToTableNameMap(Map<String, String> idToTableNameMap) {
    this.idToTableNameMap = idToTableNameMap;
  }

  public String getQueryString() {
    return queryString;
  }

  public void setQueryString(String queryString) {
    this.queryString = queryString;
  }

  public String getOptimizedQueryString() {
    return this.optimizedQueryString;
  }

  public void setOptimizedQueryString(String optimizedQueryString) {
    this.optimizedQueryString = optimizedQueryString;
  }

  public String getOptimizedCBOPlan() {
    return this.optimizedCBOPlan;
  }

  public void setOptimizedCBOPlan(String optimizedCBOPlan) {
    this.optimizedCBOPlan = optimizedCBOPlan;
  }

  public org.apache.hadoop.hive.ql.plan.api.Query getQuery() {
    return query;
  }

  public List<ReducerTimeStatsPerJob> getReducerTimeStatsPerJobList() {
    return this.reducerTimeStatsPerJobList;
  }

  public void setQuery(org.apache.hadoop.hive.ql.plan.api.Query query) {
    this.query = query;
  }

  public Map<String, Map<String, Long>> getCounters() {
    return counters;
  }

  public void setQueryId(String queryId) {
    this.queryId = queryId;
  }

  /**
   * Gets the lineage information.
   *
   * @return LineageInfo associated with the query.
   */
  public LineageInfo getLineageInfo() {
    return linfo;
  }

  /**
   * Sets the lineage information.
   *
   * @param linfo The LineageInfo structure that is set in the optimization phase.
   */
  public void setLineageInfo(LineageInfo linfo) {
    this.linfo = linfo;
  }

  /**
   * Gets the table access information.
   *
   * @return TableAccessInfo associated with the query.
   */
  public TableAccessInfo getTableAccessInfo() {
    return tableAccessInfo;
  }

  /**
   * Sets the table access information.
   *
   * @param tableAccessInfo The TableAccessInfo structure that is set right before the optimization phase.
   */
  public void setTableAccessInfo(TableAccessInfo tableAccessInfo) {
    this.tableAccessInfo = tableAccessInfo;
  }

  /**
   * Gets the column access information.
   *
   * @return ColumnAccessInfo associated with the query.
   */
  public ColumnAccessInfo getColumnAccessInfo() {
    return columnAccessInfo;
  }

  /**
   * Sets the column access information.
   *
   * @param columnAccessInfo The ColumnAccessInfo structure that is set immediately after
   * the optimization phase.
   */
  public void setColumnAccessInfo(ColumnAccessInfo columnAccessInfo) {
    this.columnAccessInfo = columnAccessInfo;
  }

  public QueryProperties getQueryProperties() {
    return queryProperties;
  }

  public Long getQueryStartTime() {
    return queryStartTime;
  }

  public void setQueryStartTime(Long queryStartTime) {
    this.queryStartTime = queryStartTime;
  }

  public String getOperationName() {
    return operation == null ? null : operation.getOperationName();
  }
  public HiveOperation getOperation() {
    return operation;
  }
  public Boolean getAutoCommitValue() {
    return autoCommitValue;
  }

  public String getCboInfo() {
    return cboInfo;
  }


}
