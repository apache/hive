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

package org.apache.hadoop.hive.ql.exec;

import static org.apache.hadoop.hive.serde.serdeConstants.STRING_TYPE_NAME;

import java.io.OutputStream;
import java.io.PrintStream;
import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.physical.StageIDsRearranger;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.ExplainWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.StringUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * ExplainTask implementation.
 *
 **/
public class ExplainTask extends Task<ExplainWork> implements Serializable {
  private static final long serialVersionUID = 1L;
  public static final String EXPL_COLUMN_NAME = "Explain";
  private Set<Operator<?>> visitedOps = new HashSet<Operator<?>>();
  private boolean isLogical = false;

  public ExplainTask() {
    super();
  }

  /*
   * Below method returns the dependencies for the passed in query to EXPLAIN.
   * The dependencies are the set of input tables and partitions, and are
   * provided back as JSON output for the EXPLAIN command.
   * Example output:
   * {"input_tables":[{"tablename": "default@test_sambavi_v1", "tabletype": "TABLE"}],
   *  "input partitions":["default@srcpart@ds=2008-04-08/hr=11"]}
   */
  private static JSONObject getJSONDependencies(ExplainWork work)
      throws Exception {
    assert(work.getDependency());

    JSONObject outJSONObject = new JSONObject();
    List<Map<String, String>> inputTableInfo = new ArrayList<Map<String, String>>();
    List<Map<String, String>> inputPartitionInfo = new ArrayList<Map<String, String>>();
    for (ReadEntity input: work.getInputs()) {
      switch (input.getType()) {
        case TABLE:
          Table table = input.getTable();
          Map<String, String> tableInfo = new HashMap<String, String>();
          tableInfo.put("tablename", table.getCompleteName());
          tableInfo.put("tabletype", table.getTableType().toString());
          if ((input.getParents() != null) && (!input.getParents().isEmpty())) {
            tableInfo.put("tableParents", input.getParents().toString());
          }
          inputTableInfo.add(tableInfo);
          break;
        case PARTITION:
          Map<String, String> partitionInfo = new HashMap<String, String>();
          partitionInfo.put("partitionName", input.getPartition().getCompleteName());
          if ((input.getParents() != null) && (!input.getParents().isEmpty())) {
            partitionInfo.put("partitionParents", input.getParents().toString());
          }
          inputPartitionInfo.add(partitionInfo);
          break;
        default:
          break;
      }
    }

    outJSONObject.put("input_tables", inputTableInfo);
    outJSONObject.put("input_partitions", inputPartitionInfo);
    return outJSONObject;
  }

  public JSONObject getJSONLogicalPlan(PrintStream out, ExplainWork work) throws Exception {
    isLogical = true;

    JSONObject outJSONObject = new JSONObject();
    boolean jsonOutput = work.isFormatted();
    if (jsonOutput) {
      out = null;
    }

    if (work.getParseContext() != null) {
      out.print("LOGICAL PLAN");
      JSONObject jsonPlan = outputMap(work.getParseContext().getTopOps(), true,
                                      out, jsonOutput, work.getExtended(), 0);
      if (out != null) {
        out.println();
      }

      if (jsonOutput) {
        outJSONObject.put("LOGICAL PLAN", jsonPlan);
      }
    } else {
      System.err.println("No parse context!");
    }
    return outJSONObject;
  }

  public JSONObject getJSONPlan(PrintStream out, ExplainWork work)
      throws Exception {
    // If the user asked for a formatted output, dump the json output
    // in the output stream
    JSONObject outJSONObject = new JSONObject();
    boolean jsonOutput = work.isFormatted();
    if (jsonOutput) {
      out = null;
    }

    // Print out the parse AST
    if (work.getAstStringTree() != null) {
      String jsonAST = outputAST(work.getAstStringTree(), out, jsonOutput, 0);
      if (out != null) {
        out.println();
      }

      if (jsonOutput) {
        outJSONObject.put("ABSTRACT SYNTAX TREE", jsonAST);
      }
    }
    List<Task<?>> tasks = work.getRootTasks();

    List<Task> ordered = StageIDsRearranger.getExplainOrder(conf, tasks);
    Task<? extends Serializable> fetchTask = work.getFetchTask();
    if (fetchTask != null) {
      fetchTask.setRootTask(true);  // todo HIVE-3925
      ordered.add(fetchTask);
    }

    JSONObject jsonDependencies = outputDependencies(out, work, ordered);

    if (out != null) {
      out.println();
    }

    if (jsonOutput) {
      outJSONObject.put("STAGE DEPENDENCIES", jsonDependencies);
    }

    // Go over all the tasks and dump out the plans
    JSONObject jsonPlan = outputStagePlans(out, work, ordered);

    if (jsonOutput) {
      outJSONObject.put("STAGE PLANS", jsonPlan);
    }

    return jsonOutput ? outJSONObject : null;
  }

  @Override
  public int execute(DriverContext driverContext) {

    PrintStream out = null;
    try {
      Path resFile = new Path(work.getResFile());
      OutputStream outS = resFile.getFileSystem(conf).create(resFile);
      out = new PrintStream(outS);

      if (work.isLogical()) {
        JSONObject jsonLogicalPlan = getJSONLogicalPlan(out, work);
        if (work.isFormatted()) {
          out.print(jsonLogicalPlan);
        }
      } else {
        if (work.getDependency()) {
          JSONObject jsonDependencies = getJSONDependencies(work);
          out.print(jsonDependencies);
        } else {
          JSONObject jsonPlan = getJSONPlan(out, work);
          if (work.isFormatted()) {
            out.print(jsonPlan);
          }
        }
      }

      out.close();
      out = null;
      return (0);
    }
    catch (Exception e) {
      console.printError("Failed with exception " + e.getMessage(),
          "\n" + StringUtils.stringifyException(e));
      return (1);
    }
    finally {
      IOUtils.closeStream(out);
    }
  }

  private String indentString(int indent) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < indent; ++i) {
      sb.append(" ");
    }

    return sb.toString();
  }

  private JSONObject outputMap(Map<?, ?> mp, boolean hasHeader, PrintStream out,
      boolean extended, boolean jsonOutput, int indent) throws Exception {

    TreeMap<Object, Object> tree = new TreeMap<Object, Object>();
    tree.putAll(mp);
    JSONObject json = jsonOutput ? new JSONObject() : null;
    if (out != null && hasHeader && !mp.isEmpty()) {
      out.println();
    }
    for (Entry<?, ?> ent : tree.entrySet()) {
      // Print the key
      if (out != null) {
        out.print(indentString(indent));
        out.printf("%s ", ent.getKey().toString());
      }

      // Print the value
      if (isPrintable(ent.getValue())) {
        if (out != null) {
          out.print(ent.getValue());
          out.println();
        }
        if (jsonOutput) {
          json.put(ent.getKey().toString(), ent.getValue().toString());
        }
      }
      else if (ent.getValue() instanceof List || ent.getValue() instanceof Map) {
        if (out != null) {
          out.print(ent.getValue().toString());
          out.println();
        }
        if (jsonOutput) {
          json.put(ent.getKey().toString(), ent.getValue().toString());
        }
      }
      else if (ent.getValue() instanceof Serializable) {
        if (out != null) {
          out.println();
        }
        JSONObject jsonOut = outputPlan((Serializable) ent.getValue(), out,
            extended, jsonOutput, jsonOutput ? 0 : indent + 2);
        if (jsonOutput) {
          json.put(ent.getKey().toString(), jsonOut);
        }
      }
      else {
        if (out != null) {
          out.println();
        }
      }
    }

    return jsonOutput ? json : null;
  }

  private JSONArray outputList(List<?> l, PrintStream out, boolean hasHeader,
      boolean extended, boolean jsonOutput, int indent) throws Exception {

    boolean first_el = true;
    boolean nl = false;
    JSONArray outputArray = new JSONArray();

    for (Object o : l) {
      if (isPrintable(o)) {
        String delim = first_el ? " " : ", ";
        if (out != null) {
          out.print(delim);
          out.print(o);
        }

        if (jsonOutput) {
          outputArray.put(o);
        }
        nl = true;
      }
      else if (o instanceof Serializable) {
        if (first_el && (out != null) && hasHeader) {
          out.println();
        }
        JSONObject jsonOut = outputPlan((Serializable) o, out, extended,
            jsonOutput, jsonOutput ? 0 : (hasHeader ? indent + 2 : indent));
        if (jsonOutput) {
          outputArray.put(jsonOut);
        }
      }

      first_el = false;
    }

    if (nl && (out != null)) {
      out.println();
    }

    return jsonOutput ? outputArray : null;
  }

  private boolean isPrintable(Object val) {
    if (val instanceof Boolean || val instanceof String
        || val instanceof Integer || val instanceof Long || val instanceof Byte
        || val instanceof Float || val instanceof Double) {
      return true;
    }

    if (val != null && val.getClass().isPrimitive()) {
      return true;
    }

    return false;
  }

  private JSONObject outputPlan(Serializable work,
      PrintStream out, boolean extended, boolean jsonOutput, int indent) throws Exception {
    return outputPlan(work, out, extended, jsonOutput, indent, "");
  }

  private JSONObject outputPlan(Serializable work, PrintStream out,
      boolean extended, boolean jsonOutput, int indent, String appendToHeader) throws Exception {
    // Check if work has an explain annotation
    Annotation note = work.getClass().getAnnotation(Explain.class);

    String keyJSONObject = null;

    if (note instanceof Explain) {
      Explain xpl_note = (Explain) note;
      if (extended || xpl_note.normalExplain()) {
        keyJSONObject = xpl_note.displayName();
        if (out != null) {
          out.print(indentString(indent));
          if (appendToHeader != null && !appendToHeader.isEmpty()) {
            out.println(xpl_note.displayName() + appendToHeader);
          } else {
            out.println(xpl_note.displayName());
          }
        }
      }
    }

    JSONObject json = jsonOutput ? new JSONObject() : null;
    // If this is an operator then we need to call the plan generation on the
    // conf and then the children
    if (work instanceof Operator) {
      Operator<? extends OperatorDesc> operator =
        (Operator<? extends OperatorDesc>) work;
      if (operator.getConf() != null) {
        String appender = isLogical ? " (" + operator.getOperatorId() + ")" : "";
        JSONObject jsonOut = outputPlan(operator.getConf(), out, extended,
            jsonOutput, jsonOutput ? 0 : indent, appender);
        if (jsonOutput) {
          json.put(operator.getOperatorId(), jsonOut);
        }
      }

      if (!visitedOps.contains(operator) || !isLogical) {
        visitedOps.add(operator);
        if (operator.getChildOperators() != null) {
          int cindent = jsonOutput ? 0 : indent + 2;
          for (Operator<? extends OperatorDesc> op : operator.getChildOperators()) {
            JSONObject jsonOut = outputPlan(op, out, extended, jsonOutput, cindent);
            if (jsonOutput) {
              json.put(operator.getOperatorId(), jsonOut);
            }
          }
        }
      }

      if (jsonOutput) {
        if (keyJSONObject != null) {
          JSONObject ret = new JSONObject();
          ret.put(keyJSONObject, json);
          return ret;
        }

        return json;
      }
      return null;
    }

    // We look at all methods that generate values for explain
    Method[] methods = work.getClass().getMethods();
    Arrays.sort(methods, new MethodComparator());

    for (Method m : methods) {
      int prop_indents = jsonOutput ? 0 : indent + 2;
      note = m.getAnnotation(Explain.class);

      if (note instanceof Explain) {
        Explain xpl_note = (Explain) note;

        if (extended || xpl_note.normalExplain()) {

          Object val = null;
          try {
            val = m.invoke(work);
          }
          catch (InvocationTargetException ex) {
            // Ignore the exception, this may be caused by external jars
            val = null;
          }

          if (val == null) {
            continue;
          }

          String header = null;
          boolean skipHeader = xpl_note.skipHeader();
          boolean emptyHeader = false;

          if (!xpl_note.displayName().equals("")) {
            header = indentString(prop_indents) + xpl_note.displayName() + ":";
          }
          else {
            emptyHeader = true;
            prop_indents = indent;
            header = indentString(prop_indents);
          }

          // Try the output as a primitive object
          if (isPrintable(val)) {
            if (out != null && shouldPrint(xpl_note, val)) {
              if (!skipHeader) {
                out.printf("%s ", header);
              }
              out.println(val);
            }
            if (jsonOutput) {
              json.put(header, val.toString());
            }
            continue;
          }

          int ind = 0;
          if (!jsonOutput) {
            if (!skipHeader) {
              ind = prop_indents + 2;
            } else {
              ind = indent;
            }
          }

          // Try this as a map
          try {
            // Go through the map and print out the stuff
            Map<?, ?> mp = (Map<?, ?>) val;

            if (out != null && !skipHeader && mp != null && !mp.isEmpty()) {
              out.print(header);
            }

            JSONObject jsonOut = outputMap(mp, !skipHeader && !emptyHeader, out, extended, jsonOutput, ind);
            if (jsonOutput) {
              json.put(header, jsonOut);
            }
            continue;
          }
          catch (ClassCastException ce) {
            // Ignore - all this means is that this is not a map
          }

          // Try this as a list
          try {
            List<?> l = (List<?>) val;

            if (out != null && !skipHeader && l != null && !l.isEmpty()) {
              out.print(header);
            }

            JSONArray jsonOut = outputList(l, out, !skipHeader && !emptyHeader, extended, jsonOutput, ind);

            if (jsonOutput) {
              json.put(header, jsonOut);
            }

            continue;
          }
          catch (ClassCastException ce) {
            // Ignore
          }

          // Finally check if it is serializable
          try {
            Serializable s = (Serializable) val;

            if (!skipHeader && out != null) {
              out.println(header);
            }
            JSONObject jsonOut = outputPlan(s, out, extended, jsonOutput, ind);
            if (jsonOutput) {
              json.put(header, jsonOut);
            }
            continue;
          }
          catch (ClassCastException ce) {
            // Ignore
          }
        }
      }
    }

    if (jsonOutput) {
      if (keyJSONObject != null) {
        JSONObject ret = new JSONObject();
        ret.put(keyJSONObject, json);
        return ret;
      }

      return json;
    }
    return null;
  }

  /**
   * use case: we want to print the object in explain only if it is true
   * how to do : print it unless the following 3 are all true:
   * 1. displayOnlyOnTrue tag is on
   * 2. object is boolean
   * 3. object is false
   * @param exp
   * @param val
   * @return
   */
  private boolean shouldPrint(Explain exp, Object val) {
    if (exp.displayOnlyOnTrue() && (val instanceof Boolean) & !((Boolean)val)) {
      return false;
    }
    return true;
  }

  private JSONObject outputPlan(Task<? extends Serializable> task,
      PrintStream out, JSONObject parentJSON, boolean extended,
      boolean jsonOutput, int indent) throws Exception {

    if (out != null) {
      out.print(indentString(indent));
      out.printf("Stage: %s\n", task.getId());
    }

    // Start by getting the work part of the task and call the output plan for
    // the work
    JSONObject jsonOutputPlan = outputPlan(task.getWork(), out, extended,
        jsonOutput, jsonOutput ? 0 : indent + 2);

    if (out != null) {
      out.println();
    }

    if (jsonOutput) {
      parentJSON.put(task.getId(), jsonOutputPlan);
    }
    return null;
  }

  private JSONObject outputDependencies(Task<? extends Serializable> task,
      PrintStream out, JSONObject parentJson, boolean jsonOutput, boolean taskType, int indent)
      throws Exception {

    boolean first = true;
    JSONObject json = jsonOutput ? new JSONObject() : null;
    if (out != null) {
      out.print(indentString(indent));
      out.printf("%s", task.getId());
    }

    if ((task.getParentTasks() == null || task.getParentTasks().isEmpty())) {
      if (task.isRootTask()) {
        if (out != null) {
          out.print(" is a root stage");
        }

        if (jsonOutput) {
          json.put("ROOT STAGE", "TRUE");
        }
      }
    }
    else {
      StringBuffer s = new StringBuffer();
      first = true;
      for (Task<? extends Serializable> parent : task.getParentTasks()) {
        if (!first) {
          s.append(", ");
        }
        first = false;
        s.append(parent.getId());
      }

      if (out != null) {
        out.print(" depends on stages: ");
        out.print(s.toString());
      }
      if (jsonOutput) {
        json.put("DEPENDENT STAGES", s.toString());
      }
    }

    Task<? extends Serializable> currBackupTask = task.getBackupTask();
    if (currBackupTask != null) {
      if (out != null) {
        out.print(" has a backup stage: ");
        out.print(currBackupTask.getId());
      }
      if (jsonOutput) {
        json.put("BACKUP STAGE", currBackupTask.getId());
      }
    }

    if (task instanceof ConditionalTask
        && ((ConditionalTask) task).getListTasks() != null) {
      StringBuffer s = new StringBuffer();
      first = true;
      for (Task<? extends Serializable> con : ((ConditionalTask) task).getListTasks()) {
        if (!first) {
          s.append(", ");
        }
        first = false;
        s.append(con.getId());
      }

      if (out != null) {
        out.print(" , consists of ");
        out.print(s.toString());
      }
      if (jsonOutput) {
        json.put("CONDITIONAL CHILD TASKS", s.toString());
      }
    }
    if (taskType) {
      if (out != null) {
        out.printf(" [%s]", task.getType());
      }
      if (jsonOutput) {
        json.put("TASK TYPE", task.getType().name());
      }
    }

    if (out != null) {
      out.println();
    }
    return jsonOutput ? json : null;
  }

  public String outputAST(String treeString, PrintStream out,
      boolean jsonOutput, int indent) throws JSONException {
    if (out != null) {
      out.print(indentString(indent));
      out.println("ABSTRACT SYNTAX TREE:");
      out.print(indentString(indent + 2));
      out.println(treeString);
    }

    return jsonOutput ? treeString : null;
  }

  public JSONObject outputDependencies(PrintStream out, ExplainWork work, List<Task> tasks)
      throws Exception {
    boolean jsonOutput = work.isFormatted();
    boolean appendTaskType = work.isAppendTaskType();
    if (out != null) {
      out.println("STAGE DEPENDENCIES:");
    }

    JSONObject json = jsonOutput ? new JSONObject() : null;
    for (Task task : tasks) {
      JSONObject jsonOut = outputDependencies(task, out, json, jsonOutput, appendTaskType, 2);
      if (jsonOutput && jsonOut != null) {
        json.put(task.getId(), jsonOut);
      }
    }

    return jsonOutput ? json : null;
  }

  public JSONObject outputStagePlans(PrintStream out, ExplainWork work, List<Task> tasks)
      throws Exception {
    boolean jsonOutput = work.isFormatted();
    if (out != null) {
      out.println("STAGE PLANS:");
    }

    JSONObject json = jsonOutput ? new JSONObject() : null;
    for (Task task : tasks) {
      outputPlan(task, out, json, work.getExtended(), jsonOutput, 2);
    }
    return jsonOutput ? json : null;
  }

  /**
   * MethodComparator.
   *
   */
  public class MethodComparator implements Comparator<Method> {
    public int compare(Method m1, Method m2) {
      return m1.getName().compareTo(m2.getName());
    }
  }

  @Override
  public StageType getType() {
    return StageType.EXPLAIN;
  }

  @Override
  public String getName() {
    return "EXPLAIN";
  }

  @Override
  public List<FieldSchema> getResultSchema() {
    FieldSchema tmpFieldSchema = new FieldSchema();
    List<FieldSchema> colList = new ArrayList<FieldSchema>();

    tmpFieldSchema.setName(EXPL_COLUMN_NAME);
    tmpFieldSchema.setType(STRING_TYPE_NAME);

    colList.add(tmpFieldSchema);
    return colList;
  }
}
