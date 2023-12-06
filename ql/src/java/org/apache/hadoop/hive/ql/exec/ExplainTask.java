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

package org.apache.hadoop.hive.ql.exec;

import static org.apache.hadoop.hive.serde.serdeConstants.STRING_TYPE_NAME;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

import com.google.common.base.Joiner;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.jsonexplain.JsonParser;
import org.apache.hadoop.hive.common.jsonexplain.JsonParserFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.Context.Operation;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.physical.StageIDsRearranger;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.ExplainConfiguration.VectorizationDetailLevel;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.Explain.Level;
import org.apache.hadoop.hive.ql.plan.Explain.Vectorization;
import org.apache.hadoop.hive.ql.plan.ExplainLockDesc;
import org.apache.hadoop.hive.ql.plan.ExplainWork;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.TezWork;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.hadoop.hive.ql.security.authorization.AuthorizationFactory;
import org.apache.hadoop.hive.ql.security.authorization.command.CommandAuthorizer;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.io.IOUtils;
import org.apache.hive.common.util.AnnotationUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.TreeSet;

/**
 * ExplainTask implementation.
 *
 **/
public class ExplainTask extends Task<ExplainWork> implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(ExplainTask.class.getName());

  public static final String STAGE_DEPENDENCIES = "STAGE DEPENDENCIES";
  private static final String EXCLUDED_RULES_PREFIX = "Excluded rules: ";
  private static final long serialVersionUID = 1L;
  public static final String EXPL_COLUMN_NAME = "Explain";
  private static final String CBO_INFO_JSON_LABEL = "cboInfo";
  private static final String CBO_PLAN_JSON_LABEL = "CBOPlan";
  private static final String CBO_PLAN_TEXT_LABEL = "CBO PLAN:";
  private final Map<Operator<?>, Integer> operatorVisits = new HashMap<>();
  private boolean isLogical = false;

  /*
   * Below method returns the dependencies for the passed in query to EXPLAIN.
   * The dependencies are the set of input tables and partitions, and are
   * provided back as JSON output for the EXPLAIN command.
   * Example output:
   * {"input_tables":[{"tablename": "default@test_sambavi_v1", "tabletype": "TABLE"}],
   *  "input partitions":["default@srcpart@ds=2008-04-08/hr=11"]}
   */
  @VisibleForTesting
  static JSONObject getJSONDependencies(ExplainWork work)
      throws Exception {
    assert(work.getDependency());

    JSONObject outJSONObject = new JSONObject(new LinkedHashMap<>());
    JSONArray inputTableInfo = new JSONArray();
    JSONArray inputPartitionInfo = new JSONArray();
    for (ReadEntity input: work.getInputs()) {
      switch (input.getType()) {
        case TABLE:
          Table table = input.getTable();
          JSONObject tableInfo = new JSONObject();
          tableInfo.put("tablename", table.getCompleteName());
          tableInfo.put("tabletype", table.getTableType().toString());
          if ((input.getParents() != null) && (!input.getParents().isEmpty())) {
            tableInfo.put("tableParents", input.getParents().toString());
          }
          inputTableInfo.put(tableInfo);
          break;
        case PARTITION:
          JSONObject partitionInfo = new JSONObject();
          partitionInfo.put("partitionName", input.getPartition().getCompleteName());
          if ((input.getParents() != null) && (!input.getParents().isEmpty())) {
            partitionInfo.put("partitionParents", input.getParents().toString());
          }
          inputPartitionInfo.put(partitionInfo);
          break;
        default:
          break;
      }
    }

    outJSONObject.put("input_tables", inputTableInfo);
    outJSONObject.put("input_partitions", inputPartitionInfo);
    return outJSONObject;
  }

  public JSONObject getJSONCBOPlan(PrintStream out, ExplainWork work) {
    JSONObject outJSONObject = new JSONObject(new LinkedHashMap<>());
    boolean jsonOutput = work.isFormatted();
    String cboPlan = work.getCboPlan();
    if (cboPlan != null) {
      String ruleExclusionRegex = getRuleExcludedRegex();
      if (jsonOutput) {
        outJSONObject.put(CBO_PLAN_JSON_LABEL, cboPlan);
        if (!ruleExclusionRegex.isEmpty()) {
          outJSONObject.put(CBO_INFO_JSON_LABEL, EXCLUDED_RULES_PREFIX + ruleExclusionRegex);
        }
      } else {
        if (!ruleExclusionRegex.isEmpty()) {
          out.println(EXCLUDED_RULES_PREFIX + ruleExclusionRegex + "\n");
        }
        out.println(CBO_PLAN_TEXT_LABEL);
        out.println(cboPlan);
      }
    }
    return outJSONObject;
  }

  public JSONObject getJSONLogicalPlan(PrintStream out, ExplainWork work) throws Exception {
    isLogical = true;

    JSONObject outJSONObject = new JSONObject(new LinkedHashMap<>());
    boolean jsonOutput = work.isFormatted();
    if (jsonOutput) {
      out = null;
    }

    if (work.getParseContext() != null) {
      if (out != null) {
        out.print("LOGICAL PLAN:");
      }
      JSONObject jsonPlan = outputMap(work.getParseContext().getTopOps(), true,
                                      out, work.getExtended(), jsonOutput, 0);
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

  private static String trueCondNameVectorizationEnabled =
      HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED.varname + " IS true";
  private static String falseCondNameVectorizationEnabled =
      HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED.varname + " IS false";

  @VisibleForTesting
  ImmutablePair<Boolean, JSONObject> outputPlanVectorization(PrintStream out, boolean jsonOutput)
      throws Exception {

    if (out != null) {
      out.println("PLAN VECTORIZATION:");
    }

    JSONObject json = jsonOutput ? new JSONObject(new LinkedHashMap<>()) : null;

    HiveConf hiveConf = queryState.getConf();

    boolean isVectorizationEnabled = HiveConf.getBoolVar(hiveConf,
        HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED);
    String isVectorizationEnabledCondName =
        (isVectorizationEnabled ?
            trueCondNameVectorizationEnabled :
              falseCondNameVectorizationEnabled);
    List<String> isVectorizationEnabledCondList = Arrays.asList(isVectorizationEnabledCondName);

    if (out != null) {
      out.print(indentString(2));
      out.print("enabled: ");
      out.println(isVectorizationEnabled);
      out.print(indentString(2));
      if (!isVectorizationEnabled) {
        out.print("enabledConditionsNotMet: ");
      } else {
        out.print("enabledConditionsMet: ");
      }
      out.println(isVectorizationEnabledCondList);
    }
    if (jsonOutput) {
      json.put("enabled", isVectorizationEnabled);
      JSONArray jsonArray = new JSONArray(Arrays.asList(isVectorizationEnabledCondName));
      if (!isVectorizationEnabled) {
        json.put("enabledConditionsNotMet", jsonArray);
      } else {
        json.put("enabledConditionsMet", jsonArray);
      }
    }

    return new ImmutablePair<Boolean, JSONObject>(isVectorizationEnabled, jsonOutput ? json : null);
  }

  public JSONObject getJSONPlan(PrintStream out, ExplainWork work)
      throws Exception {
    return getJSONPlan(out, work.getRootTasks(), work.getFetchTask(),
        work.isFormatted(), work.getExtended(), work.isAppendTaskType(), work.getCboInfo(),
        work.getCboPlan(), work.getOptimizedSQL());
  }

  public JSONObject getJSONPlan(PrintStream out, ExplainWork work, String stageIdRearrange)
          throws Exception {
    return getJSONPlan(out, work.getRootTasks(), work.getFetchTask(),
            work.isFormatted(), work.getExtended(), work.isAppendTaskType(), work.getCboInfo(),
            work.getCboPlan(), work.getOptimizedSQL(), stageIdRearrange);
  }

  public JSONObject getJSONPlan(PrintStream out, List<Task<?>> tasks, Task<?> fetchTask,
                                boolean jsonOutput, boolean isExtended, boolean appendTaskType, String cboInfo,
                                String cboPlan, String optimizedSQL) throws Exception {
    return getJSONPlan(
            out, tasks, fetchTask, jsonOutput, isExtended,
            appendTaskType, cboInfo, cboPlan, optimizedSQL,
            conf.getVar(ConfVars.HIVE_STAGE_ID_REARRANGE));
  }

  public JSONObject getJSONPlan(PrintStream out, List<Task<?>> tasks, Task<?> fetchTask,
      boolean jsonOutput, boolean isExtended, boolean appendTaskType, String cboInfo,
      String cboPlan, String optimizedSQL, String stageIdRearrange) throws Exception {

    String ruleExclusionRegex = getRuleExcludedRegex();

    // If the user asked for a formatted output, dump the json output
    // in the output stream
    JSONObject outJSONObject = new JSONObject(new LinkedHashMap<>());

    if (jsonOutput) {
      out = null;
    }

    if (cboPlan != null) {
      if (jsonOutput) {
        outJSONObject.put(CBO_PLAN_JSON_LABEL, cboPlan);
        if (!ruleExclusionRegex.isEmpty()) {
          outJSONObject.put(CBO_INFO_JSON_LABEL, EXCLUDED_RULES_PREFIX + ruleExclusionRegex);
        }
      } else {
        if (!ruleExclusionRegex.isEmpty()) {
          out.println(EXCLUDED_RULES_PREFIX + ruleExclusionRegex);
        }
        out.print(CBO_PLAN_TEXT_LABEL);
        out.println(cboPlan);
      }
    }

    if (optimizedSQL != null) {
      if (jsonOutput) {
        outJSONObject.put("optimizedSQL", optimizedSQL);
      } else {
        out.print("OPTIMIZED SQL: ");
        out.println(optimizedSQL);
      }
    }

    List<Task> ordered = StageIDsRearranger.getExplainOrder(tasks, stageIdRearrange);

    if (fetchTask != null) {
      fetchTask.setParentTasks((List)StageIDsRearranger.getFetchSources(tasks));
      if (fetchTask.getNumParent() == 0) {
        fetchTask.setRootTask(true);
      }
      ordered.add(fetchTask);
    }

    boolean suppressOthersForVectorization = false;
    if (this.work != null && (this.work.isVectorization() || this.work.isDDL())) {
      ImmutablePair<Boolean, JSONObject> planVecPair = outputPlanVectorization(out, jsonOutput);

      if (this.work.isVectorizationOnly()) {
        // Suppress the STAGES if vectorization is off.
        suppressOthersForVectorization = !planVecPair.left;
      }

      if (out != null) {
        out.println();
      }

      if (jsonOutput) {
        outJSONObject.put("PLAN VECTORIZATION", planVecPair.right);
      }
    }

    if (!suppressOthersForVectorization) {
      if (!jsonOutput && !ruleExclusionRegex.isEmpty()) {
        out.println(EXCLUDED_RULES_PREFIX + ruleExclusionRegex + "\n");
      }

      JSONObject jsonDependencies = outputDependencies(out, jsonOutput, appendTaskType, ordered);

      if (out != null) {
        out.println();
      }

      if (jsonOutput) {
        if (cboInfo != null) {
          outJSONObject.put(CBO_INFO_JSON_LABEL, cboInfo);
        }
        outJSONObject.put(STAGE_DEPENDENCIES, jsonDependencies);
      }

      // Go over all the tasks and dump out the plans
      JSONObject jsonPlan = outputStagePlans(out, ordered,
           jsonOutput, isExtended);

      if (jsonOutput) {
        outJSONObject.put("STAGE PLANS", jsonPlan);
      }

      if (fetchTask != null) {
        fetchTask.setParentTasks(null);
      }
    }

    return jsonOutput ? outJSONObject : null;
  }

  private List<String> toString(Collection<?> objects) {
    List<String> list = new ArrayList<String>();
    for (Object object : objects) {
      list.add(String.valueOf(object));
    }
    return list;
  }

  private Object toJson(String header, String message, PrintStream out, ExplainWork work)
      throws Exception {
    if (work.isFormatted()) {
      return message;
    }
    out.print(header);
    out.println(": ");
    out.print(indentString(2));
    out.println(message);
    return null;
  }

  private Object toJson(String header, List<String> messages, PrintStream out, ExplainWork work)
      throws Exception {
    if (work.isFormatted()) {
      return new JSONArray(messages);
    }
    out.print(header);
    out.println(": ");
    for (String message : messages) {
      out.print(indentString(2));
      out.print(message);
      out.println();
    }
    return null;
  }

  private JSONObject getLocks(PrintStream out, ExplainWork work) {

    JSONObject jsonObject = new JSONObject(new LinkedHashMap<>());

    boolean jsonOutput = work.isFormatted();
    if (jsonOutput) {
      out = null;
    }
    Operation operation = Optional.of(work).map(ExplainWork::getParseContext)
        .map(ParseContext::getContext).map(Context::getOperation)
        .orElse(Operation.OTHER);

    List<LockComponent> lockComponents = AcidUtils.makeLockComponents(work.getOutputs(), work.getInputs(),
        operation, conf);
    if (null != out) {
      out.print("LOCK INFORMATION:\n");
    }
    List<ExplainLockDesc> locks = new ArrayList<>(lockComponents.size());

    for (LockComponent component : lockComponents) {
      ExplainLockDesc lockDesc = new ExplainLockDesc(component);

      if (null != out) {
        out.print(lockDesc.getFullName());
        out.print(" -> ");
        out.print(lockDesc.getLockType());
        out.print('\n');
      } else {
        locks.add(lockDesc);
      }

    }

    if (jsonOutput) {
      jsonObject.put("LOCK INFORMATION:", locks);
    }
    return jsonObject;
  }

  public void addCreateTableStatement(Table table, List<String> tableCreateStmt , DDLPlanUtils ddlPlanUtils) {
    tableCreateStmt.add(ddlPlanUtils.getCreateTableCommand(table, false) + ";");
  }

  public void addPKandBasicStats(Table tbl, List<String> basicDef, DDLPlanUtils ddlPlanUtils){
    String primaryKeyStmt = ddlPlanUtils.getAlterTableStmtPrimaryKeyConstraint(tbl.getPrimaryKeyInfo());
    if (primaryKeyStmt != null) {
      basicDef.add(primaryKeyStmt);
    }
    basicDef.add(ddlPlanUtils.getAlterTableStmtTableStatsBasic(tbl));
  }

  public void addConstraints(Table tbl, List<String> constraints, DDLPlanUtils ddlPlanUtils){
    constraints.addAll(ddlPlanUtils.populateConstraints(tbl));
  }

  public void addStats(Table table,List<String> alterTableStmt ,Map<String, List<Partition>> tablePartitionsMap,
      DDLPlanUtils ddlPlanUtils)
      throws HiveException, MetaException{
    PerfLogger perfLogger = PerfLogger.getPerfLogger(conf, false);
    perfLogger.perfLogBegin(ExplainTask.class.getName(), PerfLogger.HIVE_GET_TABLE_COLUMN_STATS);
    if (table.isPartitioned()) {
      alterTableStmt.addAll(ddlPlanUtils.getDDLPlanForPartitionWithStats(table, tablePartitionsMap));
    } else {
      alterTableStmt.addAll(ddlPlanUtils.getAlterTableStmtTableStatsColsAll(table));
    }
    perfLogger.perfLogEnd(ExplainTask.class.getName(), PerfLogger.HIVE_GET_TABLE_COLUMN_STATS);
  }

  public void addExplain(String sql , List<String> explainStmt, DDLPlanUtils ddlPlanUtils){
    explainStmt.addAll(ddlPlanUtils.addExplainPlans(sql));
    return;
  }

  public void getDDLPlan(PrintStream out) throws Exception {
    DDLPlanUtils ddlPlanUtils = new DDLPlanUtils();
    Set<String> createDatabase = new TreeSet<String>();
    List<String> tableCreateStmt = new LinkedList<String>();
    List<String> tableBasicDef = new LinkedList<String>();
    List<String> createViewList = new LinkedList<String>();
    List<String> alterTableStmt = new LinkedList<String>();
    List<String> explainStmt = new LinkedList<String>();
    Map<String, Table> tableMap = new HashMap<>();
    Map<String, List<Partition>> tablePartitionsMap = new HashMap<>();
    for (ReadEntity ent : work.getInputs()) {
      switch (ent.getType()) {
      // Views are also covered in table
      case TABLE:
        Table tbl = ent.getTable();
        createDatabase.add(tbl.getDbName());
        tableMap.put(tbl.getTableName(), tbl);
        tablePartitionsMap.putIfAbsent(tbl.getTableName(), new ArrayList<Partition>());
        break;
      case PARTITION:
        tablePartitionsMap.get(ent.getTable().getTableName()).add(ent.getPartition());
        break;
      default:
        break;
      }
    }
    //process the databases
    List<String> createDatabaseStmt = ddlPlanUtils.getCreateDatabaseStmt(createDatabase);
    //process the tables
    for (String tableName : tableMap.keySet()) {
      Table table = tableMap.get(tableName);
      if (table.isView()) {
        createViewList.add(ddlPlanUtils.getCreateViewStmt(table));
        continue;
      } else {
        addCreateTableStatement(table, tableCreateStmt, ddlPlanUtils);
        addPKandBasicStats(table, tableBasicDef, ddlPlanUtils);
        addConstraints(table, alterTableStmt, ddlPlanUtils);
        addStats(table, alterTableStmt, tablePartitionsMap, ddlPlanUtils);
      }
    }
    addExplain(conf.getQueryString(), explainStmt, ddlPlanUtils);
    Joiner jn = Joiner.on("\n");
    out.println(jn.join(createDatabaseStmt));
    out.println(jn.join(tableCreateStmt));
    out.println(jn.join(tableBasicDef));
    out.println(jn.join(alterTableStmt));
    out.println(jn.join(createViewList));
    out.println(jn.join(explainStmt));
    // Get the explain plan outputs and print them in the console.
    getJSONPlan(out, work.getRootTasks(), work.getFetchTask(),
        false, false, work.isAppendTaskType(), work.getCboInfo(),
        work.getCboPlan(), work.getOptimizedSQL());
  }

  @Override
  public int execute() {

    PrintStream out = null;
    try {
      Path resFile = work.getResFile();
      OutputStream outS = resFile.getFileSystem(conf).create(resFile);
      out = new PrintStream(outS, false, StandardCharsets.UTF_8.name());

      if(work.isDDL()){
        getDDLPlan(out);
      } else if (work.isCbo()) {
        JSONObject jsonCBOPlan = getJSONCBOPlan(out, work);
        if (work.isFormatted()) {
          out.print(jsonCBOPlan);
        }
      } else if (work.isLogical()) {
        JSONObject jsonLogicalPlan = getJSONLogicalPlan(out, work);
        if (work.isFormatted()) {
          out.print(jsonLogicalPlan);
        }
      } else if (work.isAuthorize()) {
        JSONObject jsonAuth = collectAuthRelatedEntities(out, work);
        if (work.isFormatted()) {
          out.print(jsonAuth);
        }
      } else if (work.getDependency()) {
        JSONObject jsonDependencies = getJSONDependencies(work);
        out.print(jsonDependencies);
      } else if (work.isLocks()) {
        JSONObject jsonLocks = getLocks(out, work);
        if(work.isFormatted()) {
          out.print(jsonLocks);
        }
      } else if (work.isAst()) {
        // Print out the parse AST
        if (work.getAstStringTree() != null) {
          outputAST(work.getAstStringTree(), out, work.isFormatted(), 0);
        }
      } else {
        if (work.isUserLevelExplain()) {
          // Because of the implementation of the JsonParserFactory, we are sure
          // that we can get a TezJsonParser.
          JsonParser jsonParser = JsonParserFactory.getParser(conf);
          work.getConfig().setFormatted(true);
          JSONObject jsonPlan = getJSONPlan(out, work);
          if (work.getCboInfo() != null) {
            jsonPlan.put("cboInfo", work.getCboInfo());
          }
          try {
            jsonParser.print(jsonPlan, out);
          } catch (Exception e) {
            // if there is anything wrong happen, we bail out.
            LOG.error("Running explain user level has problem." +
              " Falling back to normal explain.", e);
            work.getConfig().setFormatted(false);
            work.getConfig().setUserLevelExplain(false);
            jsonPlan = getJSONPlan(out, work);
          }
        } else {
          JSONObject jsonPlan = getJSONPlan(out, work);
          if (work.isFormatted()) {
            // use the parser to get the output operators of RS
            JsonParser jsonParser = JsonParserFactory.getParser(conf);
            if (jsonParser != null) {
              jsonParser.print(jsonPlan, null);
              LOG.info("JsonPlan is augmented to {}", jsonPlan);
            }
            out.print(jsonPlan);
          }
        }
      }

      out.close();
      out = null;
      return (0);
    }
    catch (Exception e) {
      LOG.error("Failed to execute", e);
      setException(e);
      return (1);
    }
    finally {
      IOUtils.closeStream(out);
    }
  }

  @VisibleForTesting
  JSONObject collectAuthRelatedEntities(PrintStream out, ExplainWork work)
      throws Exception {

    BaseSemanticAnalyzer analyzer = work.getAnalyzer();
    HiveOperation operation = queryState.getHiveOperation();

    JSONObject object = new JSONObject(new LinkedHashMap<>());
    Object jsonInput = toJson("INPUTS", toString(analyzer.getInputs()), out, work);
    if (work.isFormatted()) {
      object.put("INPUTS", jsonInput);
    }
    Object jsonOutput = toJson("OUTPUTS", toString(analyzer.getOutputs()), out, work);
    if (work.isFormatted()) {
      object.put("OUTPUTS", jsonOutput);
    }
    String userName = SessionState.get().getAuthenticator().getUserName();
    Object jsonUser = toJson("CURRENT_USER", userName, out, work);
    if (work.isFormatted()) {
      object.put("CURRENT_USER", jsonUser);
    }
    Object jsonOperation = toJson("OPERATION", operation.name(), out, work);
    if (work.isFormatted()) {
      object.put("OPERATION", jsonOperation);
    }
    if (analyzer.skipAuthorization()) {
      return object;
    }

    final List<String> exceptions = new ArrayList<String>();
    Object delegate = SessionState.get().getActiveAuthorizer();
    if (delegate != null) {
      Class itface = SessionState.get().getAuthorizerInterface();
      Object authorizer = AuthorizationFactory.create(delegate, itface,
          new AuthorizationFactory.AuthorizationExceptionHandler() {
            @Override
            public void exception(Exception exception) {
              exceptions.add(exception.getMessage());
            }
          });

      SessionState.get().setActiveAuthorizer(authorizer);
      try {
        CommandAuthorizer.doAuthorization(queryState.getHiveOperation(), analyzer, "");
      } finally {
        SessionState.get().setActiveAuthorizer(delegate);
      }
    }
    if (!exceptions.isEmpty()) {
      Object jsonFails = toJson("AUTHORIZATION_FAILURES", exceptions, out, work);
      if (work.isFormatted()) {
        object.put("AUTHORIZATION_FAILURES", jsonFails);
      }
    }
    return object;
  }

  private static String indentString(int indent) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < indent; ++i) {
      sb.append(" ");
    }

    return sb.toString();
  }

  @VisibleForTesting
  JSONObject outputMap(Map<?, ?> mp, boolean hasHeader, PrintStream out,
      boolean extended, boolean jsonOutput, int indent) throws Exception {

    TreeMap<Object, Object> tree = getBasictypeKeyedMap(mp);
    JSONObject json = jsonOutput ? new JSONObject(new LinkedHashMap<>()) : null;
    if (out != null && hasHeader && !mp.isEmpty()) {
      out.println();
    }
    for (Entry<?, ?> ent : tree.entrySet()) {
      // Print the key
      if (out != null) {
        out.print(indentString(indent));
        out.print(ent.getKey());
        out.print(" ");
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
      else if (ent.getValue() instanceof List) {
        if (ent.getValue() != null && !((List<?>)ent.getValue()).isEmpty()
            && ((List<?>)ent.getValue()).get(0) != null &&
            ((List<?>)ent.getValue()).get(0) instanceof TezWork.Dependency) {
          if (out != null) {
            boolean isFirst = true;
            for (TezWork.Dependency dep: (List<TezWork.Dependency>)ent.getValue()) {
              if (!isFirst) {
                out.print(", ");
              } else {
                out.print("<- ");
                isFirst = false;
              }
              out.print(dep.getName());
              out.print(" (");
              out.print(dep.getType());
              out.print(")");
            }
            out.println();
          }
          if (jsonOutput) {
            for (TezWork.Dependency dep: (List<TezWork.Dependency>)ent.getValue()) {
              JSONObject jsonDep = new JSONObject(new LinkedHashMap<>());
              jsonDep.put("parent", dep.getName());
              jsonDep.put("type", dep.getType());
              json.accumulate(ent.getKey().toString(), jsonDep);
            }
          }
        } else {
          if (out != null) {
            out.print(ent.getValue().toString());
            out.println();
          }
          if (jsonOutput) {
            json.put(ent.getKey().toString(), ent.getValue().toString());
          }
        }
      }
      else if (ent.getValue() instanceof Map) {
        String stringValue = getBasictypeKeyedMap((Map)ent.getValue()).toString();
        if (out != null) {
          out.print(stringValue);
          out.println();
        }
        if (jsonOutput) {
          json.put(ent.getKey().toString(), stringValue);
        }
      }
      else if (ent.getValue() != null) {
        if (out != null) {
          out.println();
        }
        JSONObject jsonOut = outputPlan(ent.getValue(), out,
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

  /**
   * Retruns a map which have either primitive or string keys.
   *
   * This is necessary to discard object level comparators which may sort the objects based on some non-trivial logic.
   *
   * @param mp
   * @return
   */
  private TreeMap<Object, Object> getBasictypeKeyedMap(Map<?, ?> mp) {
    TreeMap<Object, Object> ret = new TreeMap<Object, Object>();
    if (mp.size() > 0) {
      Object firstKey = mp.keySet().iterator().next();
      if (firstKey.getClass().isPrimitive() || firstKey instanceof String) {
        // keep it as-is
        ret.putAll(mp);
        return ret;
      } else {
        for (Entry<?, ?> entry : mp.entrySet()) {
          // discard possibly type related sorting order and replace with alphabetical
          ret.put(entry.getKey().toString(), entry.getValue());
        }
      }
    }
    return ret;
  }

  private JSONArray outputList(List<?> l, PrintStream out, boolean hasHeader,
      boolean extended, boolean jsonOutput, int indent, boolean inTest) throws Exception {

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
      else {
        if (first_el && (out != null) && hasHeader) {
          out.println();
        }
        JSONObject jsonOut = outputPlan(o, out, extended,
            jsonOutput, jsonOutput ? 0 : (hasHeader ? indent + 2 : indent), "", inTest);
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
        || val instanceof Float || val instanceof Double || val instanceof Path) {
      return true;
    }

    if (val != null && val.getClass().isPrimitive()) {
      return true;
    }

    return false;
  }

  private JSONObject outputPlan(Object work,
      PrintStream out, boolean extended, boolean jsonOutput, int indent) throws Exception {
    return outputPlan(work, out, extended, jsonOutput, indent, "");
  }

  private boolean isInvokeVectorization(Vectorization vectorization) {

    boolean invokeFlag = true;   // Assume.

    // The EXPLAIN VECTORIZATION option was specified.
    final boolean desireOnly = this.work.isVectorizationOnly();
    final VectorizationDetailLevel desiredVecDetailLevel =
        this.work.isVectorizationDetailLevel();

    switch (vectorization) {
    case NON_VECTORIZED:
      // Display all non-vectorized leaf objects unless ONLY.
      if (desireOnly) {
        invokeFlag = false;
      }
      break;
    case SUMMARY:
    case OPERATOR:
    case EXPRESSION:
    case DETAIL:
      if (vectorization.rank < desiredVecDetailLevel.rank) {
        // This detail not desired.
        invokeFlag = false;
      }
      break;
    case SUMMARY_PATH:
    case OPERATOR_PATH:
      if (desireOnly) {
        if (vectorization.rank < desiredVecDetailLevel.rank) {
          // Suppress headers and all objects below.
          invokeFlag = false;
        }
      }
      break;
    default:
      throw new RuntimeException("Unknown EXPLAIN vectorization " + vectorization);
    }

    return invokeFlag;
  }

  private boolean isInvokeNonVectorization(Vectorization vectorization) {

    boolean invokeFlag = true;   // Assume.

    // Do not display vectorization objects.
    switch (vectorization) {
    case SUMMARY:
    case OPERATOR:
    case EXPRESSION:
    case DETAIL:
      invokeFlag = false;
      break;
    case NON_VECTORIZED:
      // No action.
      break;
    case SUMMARY_PATH:
    case OPERATOR_PATH:
      // Always include headers since they contain non-vectorized objects, too.
      break;
    default:
      throw new RuntimeException("Unknown EXPLAIN vectorization " + vectorization);
    }

    return invokeFlag;
  }

  private String getRuleExcludedRegex() {
    return conf == null ? "" : conf.get(ConfVars.HIVE_CBO_RULE_EXCLUSION_REGEX.varname, "");
  }

  @VisibleForTesting
  JSONObject outputPlan(Object work, PrintStream out,
      boolean extended, boolean jsonOutput, int indent, String appendToHeader) throws Exception {
    return outputPlan(work, out, extended, jsonOutput, indent, appendToHeader,
              queryState != null && queryState.getConf().getBoolVar(ConfVars.HIVE_IN_TEST));
  }

  public JSONObject outputPlan(Object work, PrintStream out,
                               boolean extended, boolean jsonOutput, int indent,
                               String appendToHeader, boolean inTest) throws Exception {
    // Check if work has an explain annotation
    Annotation note = AnnotationUtils.getAnnotation(work.getClass(), Explain.class);

    String keyJSONObject = null;

    if (note instanceof Explain) {
      Explain xpl_note = (Explain) note;
      boolean invokeFlag = false;
      if (this.work != null && this.work.isUserLevelExplain()) {
        invokeFlag = Level.USER.in(xpl_note.explainLevels());
      } else {
        if (extended) {
          invokeFlag = Level.EXTENDED.in(xpl_note.explainLevels());
        } else {
          invokeFlag =
              Level.DEFAULT.in(xpl_note.explainLevels()) ||
              (this.work != null && this.work.isDebug() && Level.DEBUG.in(xpl_note.explainLevels()));
        }
      }
      if (invokeFlag) {
        Vectorization vectorization = xpl_note.vectorization();
        if (this.work != null && this.work.isVectorization()) {
          invokeFlag = isInvokeVectorization(vectorization);
        } else  {
          invokeFlag = isInvokeNonVectorization(vectorization);
        }
      }
      if (invokeFlag) {
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

    JSONObject json = jsonOutput ? new JSONObject(new LinkedHashMap<>()) : null;
    // If this is an operator then we need to call the plan generation on the
    // conf and then the children
    if (work instanceof Operator) {
      Operator<? extends OperatorDesc> operator =
        (Operator<? extends OperatorDesc>) work;
      final int visitCnt = operatorVisits.merge(operator, 1, Integer::sum);
      final int limit = conf.getIntVar(ConfVars.HIVE_EXPLAIN_NODE_VISIT_LIMIT);
      if (visitCnt == limit) {
        throw new IllegalStateException(
            operator + " reached " + ConfVars.HIVE_EXPLAIN_NODE_VISIT_LIMIT.varname + "(" + limit + ")");
      }
      if (operator.getConf() != null) {
        String appender = isLogical ? " (" + operator.getOperatorId() + ")" : "";
        JSONObject jsonOut = outputPlan(operator.getConf(), out, extended,
            jsonOutput, jsonOutput ? 0 : indent, appender, inTest);
        if (this.work != null && (this.work.isUserLevelExplain() || this.work.isFormatted())) {
          if (jsonOut != null && jsonOut.length() > 0) {
            ((JSONObject) jsonOut.get(JSONObject.getNames(jsonOut)[0])).put("OperatorId:",
                operator.getOperatorId());
          }
          if (!this.work.isUserLevelExplain() && this.work.isFormatted()
              && operator.getConf() instanceof ReduceSinkDesc ) {
            ((JSONObject) jsonOut.get(JSONObject.getNames(jsonOut)[0])).put("outputname:",
                ((ReduceSinkDesc) operator.getConf()).getOutputName());
          }
        }
        if (jsonOutput) {
            json = jsonOut;
        }
      }

      if ((visitCnt == 1 || !isLogical) && operator.getChildOperators() != null) {
        int cindent = jsonOutput ? 0 : indent + 2;
        for (Operator<? extends OperatorDesc> op : operator.getChildOperators()) {
          JSONObject jsonOut = outputPlan(op, out, extended, jsonOutput, cindent, "", inTest);
          if (jsonOutput) {
            ((JSONObject) json.get(JSONObject.getNames(json)[0])).accumulate("children", jsonOut);
          }
        }
      }

      if (jsonOutput) {
        return json;
      }
      return null;
    }

    // We look at all methods that generate values for explain
    Method[] methods = work.getClass().getMethods();
    Arrays.sort(methods, new MethodComparator());

    for (Method m : methods) {
      int prop_indents = jsonOutput ? 0 : indent + 2;
      note = AnnotationUtils.getAnnotation(m, Explain.class);

      if (note instanceof Explain) {
        Explain xpl_note = (Explain) note;
        boolean invokeFlag = false;
        if (this.work != null && this.work.isUserLevelExplain()) {
          invokeFlag = Level.USER.in(xpl_note.explainLevels());
        } else {
          if (extended) {
            invokeFlag = Level.EXTENDED.in(xpl_note.explainLevels());
          } else {
            invokeFlag =
                Level.DEFAULT.in(xpl_note.explainLevels()) ||
                (this.work != null && this.work.isDebug() && Level.DEBUG.in(xpl_note.explainLevels()));
          }
        }
        if (invokeFlag) {
          Vectorization vectorization = xpl_note.vectorization();
          if (invokeFlag) {
            if (this.work != null && this.work.isVectorization()) {
              invokeFlag = isInvokeVectorization(vectorization);
            } else  {
              invokeFlag = isInvokeNonVectorization(vectorization);
            }
          }
        }
        if (invokeFlag) {

          Object val = null;
          try {
            val = m.invoke(work);
          } catch (InvocationTargetException ex) {
            // Ignore the exception, this may be caused by external jars
            val = null;
          }

          if (val == null) {
            continue;
          }

          if(xpl_note.jsonOnly() && !jsonOutput) {
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
                out.print(header);
                out.print(" ");
              }
              out.println(val);
            }
            if (jsonOutput && shouldPrint(xpl_note, val)) {
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
          if (val instanceof Map) {
            // Go through the map and print out the stuff
            Map<?, ?> mp = (Map<?, ?>) val;

            if (out != null && !skipHeader && mp != null && !mp.isEmpty()) {
              out.print(header);
            }

            JSONObject jsonOut = outputMap(mp, !skipHeader && !emptyHeader, out, extended, jsonOutput, ind);
            if (jsonOutput && !mp.isEmpty()) {
              json.put(header, jsonOut);
            }
            continue;
          }

          // Try this as a list
          if (val instanceof List || val instanceof Set) {
            List l = val instanceof List ? (List)val : new ArrayList((Set)val);
            if (out != null && !skipHeader && l != null && !l.isEmpty()) {
              out.print(header);
            }

            JSONArray jsonOut = outputList(l, out, !skipHeader && !emptyHeader, extended,
                    jsonOutput, ind, inTest);

            if (jsonOutput && !l.isEmpty()) {
              json.put(header, jsonOut);
            }

            continue;
          }

          // Finally check if it is serializable
          try {
            if (!skipHeader && out != null) {
              out.println(header);
            }
            JSONObject jsonOut = outputPlan(val, out, extended, jsonOutput, ind, "", inTest);
            if (jsonOutput && jsonOut != null && jsonOut.length() != 0) {
              if (!skipHeader) {
                json.put(header, jsonOut);
              } else {
                for(String k: JSONObject.getNames(jsonOut)) {
                  json.put(k, jsonOut.get(k));
                }
              }
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
        JSONObject ret = new JSONObject(new LinkedHashMap<>());
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

  private JSONObject outputPlan(Task<?> task,
      PrintStream out, JSONObject parentJSON, boolean extended,
      boolean jsonOutput, int indent) throws Exception {

    if (out != null) {
      out.print(indentString(indent));
      out.print("Stage: ");
      out.print(task.getId());
      out.print("\n");
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

  @VisibleForTesting
  JSONObject outputDependencies(Task<?> task,
      PrintStream out, JSONObject parentJson, boolean jsonOutput, boolean taskType, int indent)
      throws Exception {

    boolean first = true;
    JSONObject json = jsonOutput ? new JSONObject(new LinkedHashMap<>()) : null;
    if (out != null) {
      out.print(indentString(indent));
      out.print(task.getId());
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
      StringBuilder s = new StringBuilder();
      first = true;
      for (Task<?> parent : task.getParentTasks()) {
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

    Task<?> currBackupTask = task.getBackupTask();
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
      StringBuilder s = new StringBuilder();
      first = true;
      for (Task<?> con : ((ConditionalTask) task).getListTasks()) {
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
        out.print(" [");
        out.print(task.getType());
        out.print("]");
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

  public JSONObject outputDependencies(PrintStream out, boolean jsonOutput,
      boolean appendTaskType, List<Task> tasks)
      throws Exception {

    if (out != null) {
      out.println(STAGE_DEPENDENCIES + ":");
    }

    JSONObject json = jsonOutput ? new JSONObject(new LinkedHashMap<>()) : null;
    for (Task task : tasks) {
      JSONObject jsonOut = outputDependencies(task, out, json, jsonOutput, appendTaskType, 2);
      if (jsonOutput && jsonOut != null) {
        json.put(task.getId(), jsonOut);
      }
    }

    return jsonOutput ? json : null;
  }

  public JSONObject outputStagePlans(PrintStream out, List<Task> tasks,
      boolean jsonOutput, boolean isExtended)
      throws Exception {

    if (out != null) {
      out.println("STAGE PLANS:");
    }

    JSONObject json = jsonOutput ? new JSONObject(new LinkedHashMap<>()) : null;
    for (Task task : tasks) {
      outputPlan(task, out, json, isExtended, jsonOutput, 2);
    }
    return jsonOutput ? json : null;
  }

  /**
   * MethodComparator.
   *
   */
  public class MethodComparator implements Comparator<Method> {
    @Override
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

  public static List<FieldSchema> getResultSchema() {
    FieldSchema tmpFieldSchema = new FieldSchema();
    List<FieldSchema> colList = new ArrayList<FieldSchema>();

    tmpFieldSchema.setName(EXPL_COLUMN_NAME);
    tmpFieldSchema.setType(STRING_TYPE_NAME);

    colList.add(tmpFieldSchema);
    return colList;
  }

  @Override
  public boolean canExecuteInParallel() {
    return false;
  }


  /**
   * Returns EXPLAIN EXTENDED output for a semantically analyzed query.
   *
   * @param sem semantic analyzer for analyzed query
   * @param plan query plan
   * @param astTree AST tree dump
   */
  public static String getExplainOutput(BaseSemanticAnalyzer sem, QueryPlan plan, ASTNode astTree,
      QueryState queryState, Context context, HiveConf conf) throws IOException {
    String ret = null;
    ExplainTask task = new ExplainTask();
    task.initialize(queryState, plan, null, context);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(baos);
    try {
      List<Task<?>> rootTasks = sem.getAllRootTasks();
      if (conf.getBoolVar(ConfVars.HIVE_SERVER2_WEBUI_SHOW_GRAPH)) {
        JSONObject jsonPlan = task.getJSONPlan(
            null, rootTasks, sem.getFetchTask(), true,
            conf.getBoolVar(ConfVars.HIVE_LOG_EXPLAIN_OUTPUT_INCLUDE_EXTENDED), true, sem.getCboInfo(),
            plan.getOptimizedCBOPlan(), plan.getOptimizedQueryString());
        if (jsonPlan.getJSONObject(ExplainTask.STAGE_DEPENDENCIES) != null &&
            jsonPlan.getJSONObject(ExplainTask.STAGE_DEPENDENCIES).length() <=
                conf.getIntVar(ConfVars.HIVE_SERVER2_WEBUI_MAX_GRAPH_SIZE)) {
          ret = jsonPlan.toString();
        } else {
          ret = null;
        }
      } else {
        task.getJSONPlan(ps, rootTasks, sem.getFetchTask(), false, true, true, sem.getCboInfo(),
            plan.getOptimizedCBOPlan(), plan.getOptimizedQueryString());
        ret = baos.toString();
      }
    } catch (Exception e) {
      LOG.warn("Exception generating explain output: " + e, e);
    }

    return ret;
  }
}
