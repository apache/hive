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

package org.apache.hadoop.hive.ql;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.*;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.plan.api.AdjacencyType;
import org.apache.hadoop.hive.ql.plan.api.NodeType;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.hadoop.hive.ql.plan.api.TaskType;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.transport.TMemoryBuffer;
import org.apache.thrift.transport.TTransportException;


public class QueryPlan implements Serializable {
  private static final long serialVersionUID = 1L;
  
  static final private Log LOG = LogFactory.getLog(QueryPlan.class.getName());

  private String queryString;
  private BaseSemanticAnalyzer plan;
  private String queryId;
  private org.apache.hadoop.hive.ql.plan.api.Query query;
  private Map<String, Map<String, Long>> counters;
  private Set<String> done;
  private Set<String> started;

  private boolean add;


  public QueryPlan(String queryString, BaseSemanticAnalyzer plan) {
    this.queryString = queryString;
    this.plan = plan;
    this.queryId = makeQueryId();
    query = new org.apache.hadoop.hive.ql.plan.api.Query();
    query.setQueryId(this.queryId);
    query.putToQueryAttributes("queryString", this.queryString);
    counters = new HashMap<String, Map<String, Long>>();
    done = new HashSet<String>();
    started = new HashSet<String>();
  }

  public String getQueryStr() {
    return queryString;
  }

  public BaseSemanticAnalyzer getPlan() {
    return plan;
  }

  public String getQueryId() {
    return queryId;
  }

  private String makeQueryId() {
    GregorianCalendar gc = new GregorianCalendar();
    String userid = System.getProperty("user.name");

    return userid + "_" +
      String.format("%1$4d%2$02d%3$02d%4$02d%5$02d%5$02d", gc.get(Calendar.YEAR),
                    gc.get(Calendar.MONTH) + 1,
                    gc.get(Calendar.DAY_OF_MONTH),
                    gc.get(Calendar.HOUR_OF_DAY),
                    gc.get(Calendar.MINUTE), gc.get(Calendar.SECOND));
  }

  /**
   * TODO each Task should define a getType() method which will return the values
   * instead of this method
   * @param task
   * @return
   */
  private int getStageType(Task<? extends Serializable> task) {
    if (task instanceof ConditionalTask) {
      return StageType.CONDITIONAL;
    }
    if (task instanceof CopyTask) {
      return StageType.COPY;
    }
    if (task instanceof DDLTask) {
      return StageType.DDL;
    }
    if (task instanceof ExecDriver) {
      return StageType.MAPRED;
    }
    if (task instanceof ExplainTask) {
      return StageType.EXPLAIN;
    }
    if (task instanceof FetchTask) {
      return StageType.FETCH;
    }
    if (task instanceof FunctionTask) {
      return StageType.FUNC;
    }
    if (task instanceof MapRedTask) {
      return StageType.MAPREDLOCAL;
    }
    if (task instanceof MoveTask) {
      return StageType.MOVE;
    }
    assert false;
    return -1;
  }

  /**
   * TODO remove this method. add a getType() method for each operator
   * @param op
   * @return
   */
  private int getOperatorType(Operator<? extends Serializable> op) {
    if (op instanceof JoinOperator) {
      return OperatorType.JOIN;
    }
    if (op instanceof MapJoinOperator) {
      return OperatorType.MAPJOIN;
    }
    if (op instanceof ExtractOperator) {
      return OperatorType.EXTRACT;
    }
    if (op instanceof FilterOperator) {
      return OperatorType.FILTER;
    }
    if (op instanceof ForwardOperator) {
      return OperatorType.FORWARD;
    }
    if (op instanceof GroupByOperator) {
      return OperatorType.GROUPBY;
    }
    if (op instanceof LimitOperator) {
      return OperatorType.LIMIT;
    }
    if (op instanceof ScriptOperator) {
      return OperatorType.SCRIPT;
    }
    if (op instanceof SelectOperator) {
      return OperatorType.SELECT;
    }
    if (op instanceof TableScanOperator) {
      return OperatorType.TABLESCAN;
    }
    if (op instanceof FileSinkOperator) {
      return OperatorType.FILESINK;
    }
    if (op instanceof ReduceSinkOperator) {
      return OperatorType.REDUCESINK;
    }
    if (op instanceof UnionOperator) {
      return OperatorType.UNION;
    }
    assert false;
    return -1;
  }

  /**
   * generate the operator graph and operator list for the given task based on
   * the operators corresponding to that task
   * @param task   api.Task which needs its operator graph populated
   * @param topOps the set of top operators from which the operator graph for the task
   *               is hanging
   */
  private void populateOperatorGraph(org.apache.hadoop.hive.ql.plan.api.Task task,
      Collection<Operator<? extends Serializable>> topOps) {
    
    task.setOperatorGraph(new org.apache.hadoop.hive.ql.plan.api.Graph());
    task.getOperatorGraph().setNodeType(NodeType.OPERATOR);
    
    Queue<Operator<? extends Serializable>> opsToVisit = new LinkedList<Operator<? extends Serializable>>();
    Set<Operator<? extends Serializable>> opsVisited = new HashSet<Operator<? extends Serializable>>();
    opsToVisit.addAll(topOps);
    while (opsToVisit.peek() != null) {
      Operator<? extends Serializable> op = opsToVisit.remove();
      opsVisited.add(op);
      // populate the operator
      org.apache.hadoop.hive.ql.plan.api.Operator operator = new org.apache.hadoop.hive.ql.plan.api.Operator();
      operator.setOperatorId(op.getOperatorId());
      operator.setOperatorType(getOperatorType(op));
      task.addToOperatorList(operator);
      // done processing the operator
      if (op.getChildOperators() != null) {
        org.apache.hadoop.hive.ql.plan.api.Adjacency entry = new org.apache.hadoop.hive.ql.plan.api.Adjacency();
        entry.setAdjacencyType(AdjacencyType.CONJUNCTIVE);
        entry.setNode(op.getOperatorId());
        for (Operator<? extends Serializable> childOp: op.getChildOperators()) {
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

    Queue<Task<? extends Serializable>> tasksToVisit = new LinkedList<Task<? extends Serializable>>();
    Set<Task<? extends Serializable>> tasksVisited = new HashSet<Task<? extends Serializable>>();
    tasksToVisit.addAll(plan.getRootTasks());
    while (tasksToVisit.size() != 0) {
      Task<? extends Serializable> task = tasksToVisit.remove();
      tasksVisited.add(task);
      // populate stage
      org.apache.hadoop.hive.ql.plan.api.Stage stage = new org.apache.hadoop.hive.ql.plan.api.Stage();
      stage.setStageId(task.getId());
      stage.setStageType(getStageType(task));
      query.addToStageList(stage);
      
      if (task instanceof ExecDriver) {
        // populate map task
        ExecDriver mrTask = (ExecDriver)task;
        org.apache.hadoop.hive.ql.plan.api.Task mapTask = new org.apache.hadoop.hive.ql.plan.api.Task();
        mapTask.setTaskId(stage.getStageId() + "_MAP");
        mapTask.setTaskType(TaskType.MAP);
        stage.addToTaskList(mapTask);
        populateOperatorGraph(mapTask, mrTask.getWork().getAliasToWork().values());
        
        // populate reduce task
        if (mrTask.hasReduce()) {
          org.apache.hadoop.hive.ql.plan.api.Task reduceTask = new org.apache.hadoop.hive.ql.plan.api.Task();
          reduceTask.setTaskId(stage.getStageId() + "_REDUCE");
          reduceTask.setTaskType(TaskType.REDUCE);
          stage.addToTaskList(reduceTask);
          Collection<Operator<? extends Serializable>> reducerTopOps = new ArrayList<Operator<? extends Serializable>>();
          reducerTopOps.add(mrTask.getWork().getReducer());
          populateOperatorGraph(reduceTask, reducerTopOps);
        }
      }
      else {
        org.apache.hadoop.hive.ql.plan.api.Task otherTask = new org.apache.hadoop.hive.ql.plan.api.Task();
        otherTask.setTaskId(stage.getStageId() + "_OTHER");
        otherTask.setTaskType(TaskType.OTHER);
        stage.addToTaskList(otherTask);
      }
      if (task instanceof ConditionalTask) {
        org.apache.hadoop.hive.ql.plan.api.Adjacency listEntry = new org.apache.hadoop.hive.ql.plan.api.Adjacency();
        listEntry.setAdjacencyType(AdjacencyType.DISJUNCTIVE);
        listEntry.setNode(task.getId());
        ConditionalTask t = (ConditionalTask)task;
        
        for (Task<? extends Serializable> listTask: t.getListTasks()) {
          if (t.getChildTasks() != null) {
            org.apache.hadoop.hive.ql.plan.api.Adjacency childEntry = new org.apache.hadoop.hive.ql.plan.api.Adjacency();
            childEntry.setAdjacencyType(AdjacencyType.DISJUNCTIVE);
            childEntry.setNode(listTask.getId());
            // done processing the task
            for (Task<? extends Serializable> childTask: t.getChildTasks()) {
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
      }
      else if (task.getChildTasks() != null) {
        org.apache.hadoop.hive.ql.plan.api.Adjacency entry = new org.apache.hadoop.hive.ql.plan.api.Adjacency();
        entry.setAdjacencyType(AdjacencyType.CONJUNCTIVE);
        entry.setNode(task.getId());
        // done processing the task
        for (Task<? extends Serializable> childTask: task.getChildTasks()) {
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
   * From the counters extracted via extractCounters(), update the counters
   * in the query plan
   */
  private void updateCountersInQueryPlan() {
    query.setStarted(started.contains(query.getQueryId()));
    query.setDone(done.contains(query.getQueryId()));
    if (query.getStageList() != null)
    for (org.apache.hadoop.hive.ql.plan.api.Stage stage: query.getStageList()) {
      stage.setStarted(started.contains(stage.getStageId()));
      stage.setStageCounters(counters.get(stage.getStageId()));
      stage.setDone(done.contains(stage.getStageId()));
      for (org.apache.hadoop.hive.ql.plan.api.Task task: stage.getTaskList()) {
        task.setTaskCounters(counters.get(task.getTaskId()));
        if (task.getTaskType() == TaskType.OTHER) {
          task.setStarted(started.contains(stage.getStageId()));
          task.setDone(done.contains(stage.getStageId()));
        } else {
          task.setStarted(started.contains(task.getTaskId()));
          task.setDone(done.contains(task.getTaskId()));
          for (org.apache.hadoop.hive.ql.plan.api.Operator op: task.getOperatorList()) {
            // if the task has started, all operators within the task have started
            op.setStarted(started.contains(task.getTaskId()));
            op.setOperatorCounters(counters.get(op.getOperatorId()));
            // if the task is done, all operators are done as well
            op.setDone(done.contains(task.getTaskId()));
          }
        }
      }
    }
  }
  
  /**
   * extract all the counters from tasks and operators
   */
  private void extractCounters() throws IOException {
    Queue<Task<? extends Serializable>> tasksToVisit = new LinkedList<Task<? extends Serializable>>();
    Set<Task<? extends Serializable>> tasksVisited = new HashSet<Task<? extends Serializable>>();
    tasksToVisit.addAll(plan.getRootTasks());
    while (tasksToVisit.peek() != null) {
      Task<? extends Serializable> task = tasksToVisit.remove();
      tasksVisited.add(task);
      // add children to tasksToVisit
      if (task.getChildTasks() != null) {
        for (Task<? extends Serializable> childTask: task.getChildTasks()) {
          if (!tasksVisited.contains(childTask)) {
            tasksToVisit.add(childTask);
          }
        }
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
        ExecDriver mrTask = (ExecDriver)task;
        extractOperatorCounters(mrTask.getWork().getAliasToWork().values(), task.getId() + "_MAP");
        if (mrTask.mapStarted()) {
          started.add(task.getId() + "_MAP");
        }
        if (mrTask.mapDone()) {
          done.add(task.getId() + "_MAP");
        }
        if (mrTask.hasReduce()) {
          Collection<Operator<? extends Serializable>> reducerTopOps = new ArrayList<Operator<? extends Serializable>>();
          reducerTopOps.add(mrTask.getWork().getReducer());
          extractOperatorCounters(reducerTopOps, task.getId() + "_REDUCE");
          if (mrTask.reduceStarted()) {
            started.add(task.getId() + "_REDUCE");
          }
          if (mrTask.reduceDone()) {
            done.add(task.getId() + "_REDUCE");
          }
        }
      }
      else if (task instanceof ConditionalTask) {
        ConditionalTask cTask = (ConditionalTask)task;
        for (Task<? extends Serializable> listTask: cTask.getListTasks()) {
          if (!tasksVisited.contains(listTask)) {
            tasksToVisit.add(listTask);
          }
        }
      }
    }
  }

  private void extractOperatorCounters(Collection<Operator<? extends Serializable>> topOps, String taskId) {
    Queue<Operator<? extends Serializable>> opsToVisit = new LinkedList<Operator<? extends Serializable>>();
    Set<Operator<? extends Serializable>> opsVisited = new HashSet<Operator<? extends Serializable>>();
    opsToVisit.addAll(topOps);
    while (opsToVisit.size() != 0) {
      Operator<? extends Serializable> op = opsToVisit.remove();
      opsVisited.add(op);
      counters.put(op.getOperatorId(), op.getCounters());
      if (op.getDone()) {
        done.add(op.getOperatorId());
      }
      if (op.getChildOperators() != null) {
        for (Operator<? extends Serializable> childOp: op.getChildOperators()) {
          if (!opsVisited.contains(childOp)) {
            opsToVisit.add(childOp);
          }
        }
      }
    }

  }

  public org.apache.hadoop.hive.ql.plan.api.Query getQueryPlan() throws IOException {
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

  @SuppressWarnings("unchecked")
  private String getJSONList(List list) {
    if (list == null) {
      return "null";
    }
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    for (Object entry: list) {
      sb.append(getJSONValue(entry));
      sb.append(",");
    }
    sb.deleteCharAt(sb.length()-1);
    sb.append("]");
    return sb.toString();
  }

  @SuppressWarnings("unchecked")
  public String getJSONMap(Map map) {
    if (map == null) {
      return "null";
    }
    StringBuilder sb = new StringBuilder();
    sb.append("{");
    for (Object entry: map.entrySet()) {
      Map.Entry e = (Map.Entry)entry;
      sb.append(getJSONKeyValue(e.getKey(), e.getValue()));
    }
    sb.deleteCharAt(sb.length()-1);
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
      for (org.apache.hadoop.hive.ql.plan.api.Adjacency adj: graph.getAdjacencyList()) {
        adjList.add(getJSONAdjacency(adj));
      }
    }
    sb.append(getJSONKeyValue("adjacencyList", getJSONList(adjList)));
    sb.deleteCharAt(sb.length()-1);
    sb.append("}");
    return sb.toString();
  }

  private String getJSONAdjacency(org.apache.hadoop.hive.ql.plan.api.Adjacency adj) {
    if (adj == null) {
      return "null";
    }
    StringBuilder sb = new StringBuilder();
    sb.append("{");
    sb.append(getJSONKeyValue("node", adj.getNode()));
    sb.append(getJSONKeyValue("children", getJSONList(adj.getChildren())));
    sb.append(getJSONKeyValue("adjacencyType", adj.getAdjacencyType()));
    sb.deleteCharAt(sb.length()-1);
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
    sb.deleteCharAt(sb.length()-1);
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
      for (org.apache.hadoop.hive.ql.plan.api.Operator op: task.getOperatorList()) {
        opList.add(getJSONOperator(op));
      }
    }
    sb.append(getJSONKeyValue("operatorList", getJSONList(opList)));
    sb.append(getJSONKeyValue("done", task.isDone()));
    sb.append(getJSONKeyValue("started", task.isStarted()));
    sb.deleteCharAt(sb.length()-1);
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
      for (org.apache.hadoop.hive.ql.plan.api.Task task: stage.getTaskList()) {
        taskList.add(getJSONTask(task));
      }
    }
    sb.append(getJSONKeyValue("taskList", getJSONList(taskList)));
    sb.append(getJSONKeyValue("done", stage.isDone()));
    sb.append(getJSONKeyValue("started", stage.isStarted()));
    sb.deleteCharAt(sb.length()-1);
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
      for (org.apache.hadoop.hive.ql.plan.api.Stage stage: query.getStageList()) {
        stageList.add(getJSONStage(stage));
      }
    }
    sb.append(getJSONKeyValue("stageList", getJSONList(stageList)));
    sb.append(getJSONKeyValue("done", query.isDone()));
    sb.append(getJSONKeyValue("started", query.isStarted()));
    sb.deleteCharAt(sb.length()-1);
    sb.append("}");
    return sb.toString();
  }

  @Override
  public String toString() {
    try {
      return getJSONQuery(getQueryPlan());
    }
    catch (Exception e) {
      e.printStackTrace();
      return e.toString();
    }
  } 

  public String toThriftJSONString() throws IOException {
    org.apache.hadoop.hive.ql.plan.api.Query q = getQueryPlan();
    TMemoryBuffer tmb = new TMemoryBuffer(q.toString().length()*5);
    TJSONProtocol oprot = new TJSONProtocol(tmb);
    try {
      q.write(oprot);
    } catch (TException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      return q.toString();
    }
    return tmb.toString("UTF-8");
  }

  public String toBinaryString() throws IOException {
    org.apache.hadoop.hive.ql.plan.api.Query q = getQueryPlan();
    TMemoryBuffer tmb = new TMemoryBuffer(q.toString().length()*5);
    TBinaryProtocol oprot = new TBinaryProtocol(tmb);
    try {
      q.write(oprot);
    } catch (TException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      return q.toString();
    }
    byte[] buf = new byte[tmb.length()];
    tmb.read(buf, 0, tmb.length());
    return new String(buf);
    //return getQueryPlan().toString();
    
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

}
