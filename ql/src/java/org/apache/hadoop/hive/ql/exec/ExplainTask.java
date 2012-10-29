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

import static org.apache.hadoop.hive.serde.Constants.STRING_TYPE_NAME;

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
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.ExplainWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.StringUtils;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * ExplainTask implementation.
 *
 **/
public class ExplainTask extends Task<ExplainWork> implements Serializable {
  private static final long serialVersionUID = 1L;
  public static final String EXPL_COLUMN_NAME = "Explain";
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
    Set<String> inputPartitions = new HashSet<String>();
    Set<String> inputTables = new HashSet<String>();
    Table table = null;
    for (ReadEntity input: work.getInputs()) {
      switch (input.getType()) {
        case TABLE:
          table = input.getTable();
          break;
        case PARTITION:
          inputPartitions.add(input.getPartition().getCompleteName());
          table = input.getPartition().getTable();
          break;
        default:
          table = null;
          break;
      }

      if (table != null && !inputTables.contains(table.getCompleteName())) {
        Map<String, String> tableInfo = new HashMap<String, String>();
        tableInfo.put("tablename", table.getCompleteName());
        tableInfo.put("tabletype", table.getTableType().toString());
        inputTableInfo.add(tableInfo);
        inputTables.add(table.getCompleteName());
      }
    }

    outJSONObject.put("input_tables", inputTableInfo);
    outJSONObject.put("input_partitions", inputPartitions);
    return outJSONObject;
  }

  static public JSONObject getJSONPlan(PrintStream out, ExplainWork work)
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

    JSONObject jsonDependencies = outputDependencies(out, jsonOutput,
        work.getRootTasks(), 0);

    if (out != null) {
      out.println();
    }

    if (jsonOutput) {
      outJSONObject.put("STAGE DEPENDENCIES", jsonDependencies);
    }

    // Go over all the tasks and dump out the plans
    JSONObject jsonPlan = outputStagePlans(out, work, work.getRootTasks(), 0);

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

      if (work.getDependency()) {
        JSONObject jsonDependencies = getJSONDependencies(work);
        out.print(jsonDependencies);
      } else {
        JSONObject jsonPlan = getJSONPlan(out, work);
        if (work.isFormatted()) {
          out.print(jsonPlan);
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

  private static String indentString(int indent) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < indent; ++i) {
      sb.append(" ");
    }

    return sb.toString();
  }

  private static JSONObject outputMap(Map<?, ?> mp, String header, PrintStream out,
      boolean extended, boolean jsonOutput, int indent) throws Exception {

    boolean first_el = true;
    TreeMap<Object, Object> tree = new TreeMap<Object, Object>();
    tree.putAll(mp);
    JSONObject json = jsonOutput ? new JSONObject() : null;
    for (Entry<?, ?> ent : tree.entrySet()) {
      if (first_el && (out != null)) {
        out.println(header);
      }
      first_el = false;

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

  private static String outputList(List<?> l, String header, PrintStream out,
      boolean extended, boolean jsonOutput, int indent) throws Exception {

    boolean first_el = true;
    boolean nl = false;
    StringBuffer s = new StringBuffer();
    for (Object o : l) {
      if (first_el && (out != null)) {
        out.print(header);
      }

      if (isPrintable(o)) {
        String delim = first_el ? " " : ", ";
        if (out != null) {
          out.print(delim);
          out.print(o);
        }

        if (jsonOutput) {
          s.append(delim);
          s.append(o);
        }
        nl = true;
      }
      else if (o instanceof Serializable) {
        if (first_el && (out != null)) {
          out.println();
        }
        JSONObject jsonOut = outputPlan((Serializable) o, out, extended,
            jsonOutput, jsonOutput ? 0 : indent + 2);
        if (jsonOutput) {
          if (!first_el) {
            s.append(", ");
          }
          s.append(jsonOut);
        }
      }

      first_el = false;
    }

    if (nl && (out != null)) {
      out.println();
    }
    return jsonOutput ? s.toString() : null;
  }

  private static boolean isPrintable(Object val) {
    if (val instanceof Boolean || val instanceof String
        || val instanceof Integer || val instanceof Byte
        || val instanceof Float || val instanceof Double) {
      return true;
    }

    if (val != null && val.getClass().isPrimitive()) {
      return true;
    }

    return false;
  }

  private static JSONObject outputPlan(Serializable work, PrintStream out,
      boolean extended, boolean jsonOutput, int indent) throws Exception {
    // Check if work has an explain annotation
    Annotation note = work.getClass().getAnnotation(Explain.class);

    String keyJSONObject = null;

    if (note instanceof Explain) {
      Explain xpl_note = (Explain) note;
      if (extended || xpl_note.normalExplain()) {
        keyJSONObject = xpl_note.displayName();
        if (out != null) {
          out.print(indentString(indent));
          out.println(xpl_note.displayName());
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
        JSONObject jsonOut = outputPlan(operator.getConf(), out, extended,
            jsonOutput, jsonOutput ? 0 : indent);
        if (jsonOutput) {
          json.put(operator.getOperatorId(), jsonOut);
        }
      }

      if (operator.getChildOperators() != null) {
        for (Operator<? extends OperatorDesc> op : operator.getChildOperators()) {
          JSONObject jsonOut = outputPlan(op, out, extended, jsonOutput, jsonOutput ? 0 : indent + 2);
          if (jsonOutput) {
            json.put(operator.getOperatorId(), jsonOut);
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
          if (!xpl_note.displayName().equals("")) {
            header = indentString(prop_indents) + xpl_note.displayName() + ":";
          }
          else {
            prop_indents = indent;
            header = indentString(prop_indents);
          }

          if (isPrintable(val)) {
            if (out != null) {
              out.printf("%s ", header);
              out.println(val);
            }
            if (jsonOutput) {
              json.put(header, val.toString());
            }
            continue;
          }
          // Try this as a map
          try {
            // Go through the map and print out the stuff
            Map<?, ?> mp = (Map<?, ?>) val;
            JSONObject jsonOut = outputMap(mp, header, out, extended, jsonOutput,
                jsonOutput ? 0 : prop_indents + 2);
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
            String jsonOut = outputList(l, header, out, extended, jsonOutput,
                jsonOutput ? 0 : prop_indents + 2);
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
            if (out != null) {
              out.println(header);
            }
            JSONObject jsonOut = outputPlan(s, out, extended, jsonOutput,
                jsonOutput ? 0 : prop_indents + 2);
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

  private static JSONObject outputPlan(Task<? extends Serializable> task,
      PrintStream out, JSONObject parentJSON, boolean extended,
      boolean jsonOutput, HashSet<Task<? extends Serializable>> displayedSet,
      int indent) throws Exception {

    if (displayedSet.contains(task)) {
      return null;
    }
    displayedSet.add(task);

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

    if (task instanceof ConditionalTask
        && ((ConditionalTask) task).getListTasks() != null) {
      for (Task<? extends Serializable> con : ((ConditionalTask) task).getListTasks()) {
        outputPlan(con, out, parentJSON, extended, jsonOutput, displayedSet,
            jsonOutput ? 0 : indent);
      }
    }
    if (task.getChildTasks() != null) {
      for (Task<? extends Serializable> child : task.getChildTasks()) {
        outputPlan(child, out, parentJSON, extended, jsonOutput, displayedSet,
            jsonOutput ? 0 : indent);
      }
    }
    return null;
  }

  private static JSONObject outputDependencies(Task<? extends Serializable> task,
      Set<Task<? extends Serializable>> dependeciesTaskSet, PrintStream out,
      JSONObject parentJson, boolean jsonOutput, int indent,
      boolean rootTskCandidate) throws Exception {

    if (dependeciesTaskSet.contains(task)) {
      return null;
    }
    dependeciesTaskSet.add(task);
    boolean first = true;
    JSONObject json = jsonOutput ? new JSONObject() : null;
    if (out != null) {
      out.print(indentString(indent));
      out.printf("%s", task.getId());
    }

    if ((task.getParentTasks() == null || task.getParentTasks().isEmpty())) {
      if (rootTskCandidate) {
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

    if (out != null) {
      out.println();
    }

    if (task instanceof ConditionalTask
        && ((ConditionalTask) task).getListTasks() != null) {
      for (Task<? extends Serializable> con : ((ConditionalTask) task).getListTasks()) {
        JSONObject jsonOut = outputDependencies(con, dependeciesTaskSet, out,
            parentJson, jsonOutput, jsonOutput ? 0 : indent, false);
        if (jsonOutput && (jsonOut != null)) {
          parentJson.put(con.getId(), jsonOut);
        }
      }
    }

    if (task.getChildTasks() != null) {
      for (Task<? extends Serializable> child : task.getChildTasks()) {
        JSONObject jsonOut = outputDependencies(child, dependeciesTaskSet, out,
            parentJson, jsonOutput, jsonOutput ? 0 : indent, true);
        if (jsonOutput && (jsonOut != null)) {
          parentJson.put(child.getId(), jsonOut);
        }
      }
    }
    return jsonOutput ? json : null;
  }

  public static String outputAST(String treeString, PrintStream out,
      boolean jsonOutput, int indent) throws JSONException {
    if (out != null) {
      out.print(indentString(indent));
      out.println("ABSTRACT SYNTAX TREE:");
      out.print(indentString(indent + 2));
      out.println(treeString);
    }

    return jsonOutput ? treeString : null;
  }

  public static JSONObject outputDependencies(PrintStream out, boolean jsonOutput,
      List<Task<? extends Serializable>> rootTasks, int indent)
      throws Exception {
    if (out != null) {
      out.print(indentString(indent));
      out.println("STAGE DEPENDENCIES:");
    }

    JSONObject json = jsonOutput ? new JSONObject() : null;
    Set<Task<? extends Serializable>> dependenciesTaskSet =
      new HashSet<Task<? extends Serializable>>();

    for (Task<? extends Serializable> rootTask : rootTasks) {
      JSONObject jsonOut = outputDependencies(rootTask,
          dependenciesTaskSet, out, json, jsonOutput,
          jsonOutput ? 0 : indent + 2, true);
      if (jsonOutput && (jsonOut != null)) {
        json.put(rootTask.getId(), jsonOut);
      }
    }

    return jsonOutput ? json : null;
  }

  public static JSONObject outputStagePlans(PrintStream out, ExplainWork work,
      List<Task<? extends Serializable>> rootTasks, int indent)
      throws Exception {
    boolean jsonOutput = work.isFormatted();
    if (out != null) {
      out.print(indentString(indent));
      out.println("STAGE PLANS:");
    }

    JSONObject json = jsonOutput ? new JSONObject() : null;
    HashSet<Task<? extends Serializable>> displayedSet = new HashSet<Task<? extends Serializable>>();
    for (Task<? extends Serializable> rootTask : rootTasks) {
      outputPlan(rootTask, out, json, work.getExtended(), jsonOutput,
          displayedSet, jsonOutput ? 0 : indent + 2);
    }
    return jsonOutput ? json : null;
  }

  /**
   * MethodComparator.
   *
   */
  public static class MethodComparator implements Comparator {
    public int compare(Object o1, Object o2) {
      Method m1 = (Method) o1;
      Method m2 = (Method) o2;
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
  protected void localizeMRTmpFilesImpl(Context ctx) {
    // explain task has nothing to localize
    // we don't expect to enter this code path at all
    throw new RuntimeException("Unexpected call");
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
