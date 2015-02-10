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

package org.apache.hadoop.hive.ql.parse;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVESTATSDBCLASS;

import java.io.IOException;
import java.io.Serializable;
import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.antlr.runtime.ClassicToken;
import org.antlr.runtime.CommonToken;
import org.antlr.runtime.Token;
import org.antlr.runtime.tree.Tree;
import org.antlr.runtime.tree.TreeWizard;
import org.antlr.runtime.tree.TreeWizard.ContextVisitor;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.common.StatsSetupConst.StatDB;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryProperties;
import org.apache.hadoop.hive.ql.exec.AbstractMapJoinOperator;
import org.apache.hadoop.hive.ql.exec.ArchiveUtils;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluatorFactory;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.RecordReader;
import org.apache.hadoop.hive.ql.exec.RecordWriter;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.SMBMapJoinOperator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.UnionOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.io.AcidOutputFormat;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.io.NullRowsInputFormat;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.metadata.DummyPartition;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.metadata.InvalidTableException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.optimizer.Optimizer;
import org.apache.hadoop.hive.ql.optimizer.calcite.CalciteSemanticException;
import org.apache.hadoop.hive.ql.optimizer.unionproc.UnionProcContext;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer.tableSpec.SpecType;
import org.apache.hadoop.hive.ql.parse.CalcitePlanner.ASTSearcher;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.OrderExpression;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.OrderSpec;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.PTFInputSpec;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.PTFQueryInputSpec;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.PTFQueryInputType;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.PartitionExpression;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.PartitionSpec;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.PartitionedTableFunctionSpec;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.PartitioningSpec;
import org.apache.hadoop.hive.ql.parse.QBSubQuery.SubQueryType;
import org.apache.hadoop.hive.ql.parse.SubQueryUtils.ISubQueryJoinInfo;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.BoundarySpec;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.CurrentRowSpec;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.Direction;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.RangeBoundarySpec;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.ValueBoundarySpec;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.WindowExpressionSpec;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.WindowFrameSpec;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.WindowFunctionSpec;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.WindowSpec;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.CreateTableDesc;
import org.apache.hadoop.hive.ql.plan.CreateTableLikeDesc;
import org.apache.hadoop.hive.ql.plan.CreateViewDesc;
import org.apache.hadoop.hive.ql.plan.DDLWork;
import org.apache.hadoop.hive.ql.plan.DynamicPartitionCtx;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnListDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.ExtractDesc;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.FilterDesc;
import org.apache.hadoop.hive.ql.plan.FilterDesc.sampleDesc;
import org.apache.hadoop.hive.ql.plan.ForwardDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.plan.JoinCondDesc;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.LateralViewForwardDesc;
import org.apache.hadoop.hive.ql.plan.LateralViewJoinDesc;
import org.apache.hadoop.hive.ql.plan.LimitDesc;
import org.apache.hadoop.hive.ql.plan.ListBucketingCtx;
import org.apache.hadoop.hive.ql.plan.LoadFileDesc;
import org.apache.hadoop.hive.ql.plan.LoadTableDesc;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PTFDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.ScriptDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.ql.plan.UDTFDesc;
import org.apache.hadoop.hive.ql.plan.UnionDesc;
import org.apache.hadoop.hive.ql.plan.ptf.OrderExpressionDef;
import org.apache.hadoop.hive.ql.plan.ptf.PTFExpressionDef;
import org.apache.hadoop.hive.ql.plan.ptf.PartitionedTableFunctionDef;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.ResourceType;
import org.apache.hadoop.hive.ql.stats.StatsFactory;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.Mode;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFHash;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPOr;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.MetadataTypedColumnsetSerDe;
import org.apache.hadoop.hive.serde2.NullStructSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.shims.HadoopShims;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Implementation of the semantic analyzer. It generates the query plan.
 * There are other specific semantic analyzers for some hive operations such as
 * DDLSemanticAnalyzer for ddl operations.
 */

public class SemanticAnalyzer extends BaseSemanticAnalyzer {

  public static final String DUMMY_DATABASE = "_dummy_database";
  public static final String DUMMY_TABLE = "_dummy_table";
  public static final String SUBQUERY_TAG_1 = "-subquery1";
  public static final String SUBQUERY_TAG_2 = "-subquery2";

  // Max characters when auto generating the column name with func name
  private static final int AUTOGEN_COLALIAS_PRFX_MAXLENGTH = 20;

  private static final String VALUES_TMP_TABLE_NAME_PREFIX = "Values__Tmp__Table__";

  private HashMap<TableScanOperator, ExprNodeDesc> opToPartPruner;
  private HashMap<TableScanOperator, PrunedPartitionList> opToPartList;
  private HashMap<String, Operator<? extends OperatorDesc>> topOps;
  private HashMap<String, Operator<? extends OperatorDesc>> topSelOps;
  private LinkedHashMap<Operator<? extends OperatorDesc>, OpParseContext> opParseCtx;
  private List<LoadTableDesc> loadTableWork;
  private List<LoadFileDesc> loadFileWork;
  private Map<JoinOperator, QBJoinTree> joinContext;
  private Map<SMBMapJoinOperator, QBJoinTree> smbMapJoinContext;
  private final HashMap<TableScanOperator, Table> topToTable;
  private final Map<FileSinkOperator, Table> fsopToTable;
  private final List<ReduceSinkOperator> reduceSinkOperatorsAddedByEnforceBucketingSorting;
  private final HashMap<TableScanOperator, Map<String, String>> topToTableProps;
  private QB qb;
  private ASTNode ast;
  private int destTableId;
  private UnionProcContext uCtx;
  List<AbstractMapJoinOperator<? extends MapJoinDesc>> listMapJoinOpsNoReducer;
  private HashMap<TableScanOperator, sampleDesc> opToSamplePruner;
  private final Map<TableScanOperator, Map<String, ExprNodeDesc>> opToPartToSkewedPruner;
  /**
   * a map for the split sampling, from alias to an instance of SplitSample
   * that describes percentage and number.
   */
  private final HashMap<String, SplitSample> nameToSplitSample;
  Map<GroupByOperator, Set<String>> groupOpToInputTables;
  Map<String, PrunedPartitionList> prunedPartitions;
  private List<FieldSchema> resultSchema;
  private CreateViewDesc createVwDesc;
  private ArrayList<String> viewsExpanded;
  private ASTNode viewSelect;
  private final UnparseTranslator unparseTranslator;
  private final GlobalLimitCtx globalLimitCtx;

  // prefix for column names auto generated by hive
  private final String autogenColAliasPrfxLbl;
  private final boolean autogenColAliasPrfxIncludeFuncName;

  // Keep track of view alias to read entity corresponding to the view
  // For eg: for a query like 'select * from V3', where V3 -> V2, V2 -> V1, V1 -> T
  // keeps track of aliases for V3, V3:V2, V3:V2:V1.
  // This is used when T is added as an input for the query, the parents of T is
  // derived from the alias V3:V2:V1:T
  private final Map<String, ReadEntity> viewAliasToInput;

  // flag for no scan during analyze ... compute statistics
  protected boolean noscan;

  //flag for partial scan during analyze ... compute statistics
  protected boolean partialscan;

  protected volatile boolean disableJoinMerge = false;

  /*
   * Capture the CTE definitions in a Query.
   */
  private final Map<String, ASTNode> aliasToCTEs;
  /*
   * Used to check recursive CTE invocations. Similar to viewsExpanded
   */
  private ArrayList<String> ctesExpanded;

  /** Not thread-safe. */
  final ASTSearcher astSearcher = new ASTSearcher();

  static class Phase1Ctx {
    String dest;
    int nextNum;
  }

  public SemanticAnalyzer(HiveConf conf) throws SemanticException {
    super(conf);
    opToPartPruner = new HashMap<TableScanOperator, ExprNodeDesc>();
    opToPartList = new HashMap<TableScanOperator, PrunedPartitionList>();
    opToSamplePruner = new HashMap<TableScanOperator, sampleDesc>();
    nameToSplitSample = new HashMap<String, SplitSample>();
    // Must be deterministic order maps - see HIVE-8707
    topOps = new LinkedHashMap<String, Operator<? extends OperatorDesc>>();
    topSelOps = new LinkedHashMap<String, Operator<? extends OperatorDesc>>();
    loadTableWork = new ArrayList<LoadTableDesc>();
    loadFileWork = new ArrayList<LoadFileDesc>();
    opParseCtx = new LinkedHashMap<Operator<? extends OperatorDesc>, OpParseContext>();
    joinContext = new HashMap<JoinOperator, QBJoinTree>();
    smbMapJoinContext = new HashMap<SMBMapJoinOperator, QBJoinTree>();
    // Must be deterministic order map for consistent q-test output across Java versions
    topToTable = new LinkedHashMap<TableScanOperator, Table>();
    fsopToTable = new HashMap<FileSinkOperator, Table>();
    reduceSinkOperatorsAddedByEnforceBucketingSorting = new ArrayList<ReduceSinkOperator>();
    topToTableProps = new HashMap<TableScanOperator, Map<String, String>>();
    destTableId = 1;
    uCtx = null;
    listMapJoinOpsNoReducer = new ArrayList<AbstractMapJoinOperator<? extends MapJoinDesc>>();
    groupOpToInputTables = new HashMap<GroupByOperator, Set<String>>();
    prunedPartitions = new HashMap<String, PrunedPartitionList>();
    unparseTranslator = new UnparseTranslator(conf);
    autogenColAliasPrfxLbl = HiveConf.getVar(conf,
        HiveConf.ConfVars.HIVE_AUTOGEN_COLUMNALIAS_PREFIX_LABEL);
    autogenColAliasPrfxIncludeFuncName = HiveConf.getBoolVar(conf,
        HiveConf.ConfVars.HIVE_AUTOGEN_COLUMNALIAS_PREFIX_INCLUDEFUNCNAME);
    queryProperties = new QueryProperties();
    opToPartToSkewedPruner = new HashMap<TableScanOperator, Map<String, ExprNodeDesc>>();
    aliasToCTEs = new HashMap<String, ASTNode>();
    globalLimitCtx = new GlobalLimitCtx();
    viewAliasToInput = new HashMap<String, ReadEntity>();
    noscan = partialscan = false;
  }

  @Override
  protected void reset(boolean clearPartsCache) {
    super.reset(true);
    if(clearPartsCache) {
      prunedPartitions.clear();
    }
    loadTableWork.clear();
    loadFileWork.clear();
    topOps.clear();
    topSelOps.clear();
    destTableId = 1;
    idToTableNameMap.clear();
    qb = null;
    ast = null;
    uCtx = null;
    joinContext.clear();
    smbMapJoinContext.clear();
    opParseCtx.clear();
    groupOpToInputTables.clear();
    disableJoinMerge = false;
    aliasToCTEs.clear();
    topToTable.clear();
    opToPartPruner.clear();
    opToPartList.clear();
    opToPartToSkewedPruner.clear();
    opToSamplePruner.clear();
    nameToSplitSample.clear();
    fsopToTable.clear();
    resultSchema = null;
    createVwDesc = null;
    viewsExpanded = null;
    viewSelect = null;
    ctesExpanded = null;
    globalLimitCtx.disableOpt();
    viewAliasToInput.clear();
    reduceSinkOperatorsAddedByEnforceBucketingSorting.clear();
    topToTableProps.clear();
    listMapJoinOpsNoReducer.clear();
    unparseTranslator.clear();
    queryProperties.clear();
    outputs.clear();
  }

  public void initParseCtx(ParseContext pctx) {
    opToPartPruner = pctx.getOpToPartPruner();
    opToPartList = pctx.getOpToPartList();
    opToSamplePruner = pctx.getOpToSamplePruner();
    topOps = pctx.getTopOps();
    opParseCtx = pctx.getOpParseCtx();
    loadTableWork = pctx.getLoadTableWork();
    loadFileWork = pctx.getLoadFileWork();
    ctx = pctx.getContext();
    destTableId = pctx.getDestTableId();
    idToTableNameMap = pctx.getIdToTableNameMap();
    uCtx = pctx.getUCtx();
    listMapJoinOpsNoReducer = pctx.getListMapJoinOpsNoReducer();
    qb = pctx.getQB();
    groupOpToInputTables = pctx.getGroupOpToInputTables();
    prunedPartitions = pctx.getPrunedPartitions();
    fetchTask = pctx.getFetchTask();
    setLineageInfo(pctx.getLineageInfo());
  }

  public ParseContext getParseContext() {
    return new ParseContext(conf, qb, ast, opToPartPruner, opToPartList,
        topOps, opParseCtx,
        new HashSet<JoinOperator>(joinContext.keySet()),
        new HashSet<SMBMapJoinOperator>(smbMapJoinContext.keySet()),
        loadTableWork, loadFileWork, ctx, idToTableNameMap, destTableId, uCtx,
        listMapJoinOpsNoReducer, groupOpToInputTables, prunedPartitions,
        opToSamplePruner, globalLimitCtx, nameToSplitSample, inputs, rootTasks,
        opToPartToSkewedPruner, viewAliasToInput,
        reduceSinkOperatorsAddedByEnforceBucketingSorting, queryProperties);
  }

  @SuppressWarnings("nls")
  public void doPhase1QBExpr(ASTNode ast, QBExpr qbexpr, String id, String alias)
      throws SemanticException {

    assert (ast.getToken() != null);
    switch (ast.getToken().getType()) {
    case HiveParser.TOK_QUERY: {
      QB qb = new QB(id, alias, true);
      Phase1Ctx ctx_1 = initPhase1Ctx();
      doPhase1(ast, qb, ctx_1, null);

      qbexpr.setOpcode(QBExpr.Opcode.NULLOP);
      qbexpr.setQB(qb);
    }
      break;
    case HiveParser.TOK_UNION: {
      qbexpr.setOpcode(QBExpr.Opcode.UNION);
      // query 1
      assert (ast.getChild(0) != null);
      QBExpr qbexpr1 = new QBExpr(alias + "-subquery1");
      doPhase1QBExpr((ASTNode) ast.getChild(0), qbexpr1, id + "-subquery1",
          alias + "-subquery1");
      qbexpr.setQBExpr1(qbexpr1);

      // query 2
      assert (ast.getChild(0) != null);
      QBExpr qbexpr2 = new QBExpr(alias + "-subquery2");
      doPhase1QBExpr((ASTNode) ast.getChild(1), qbexpr2, id + "-subquery2",
          alias + "-subquery2");
      qbexpr.setQBExpr2(qbexpr2);
    }
      break;
    }
  }

  private LinkedHashMap<String, ASTNode> doPhase1GetAggregationsFromSelect(
      ASTNode selExpr, QB qb, String dest) throws SemanticException {

    // Iterate over the selects search for aggregation Trees.
    // Use String as keys to eliminate duplicate trees.
    LinkedHashMap<String, ASTNode> aggregationTrees = new LinkedHashMap<String, ASTNode>();
    List<ASTNode> wdwFns = new ArrayList<ASTNode>();
    for (int i = 0; i < selExpr.getChildCount(); ++i) {
      ASTNode function = (ASTNode) selExpr.getChild(i);
      if (function.getType() == HiveParser.TOK_SELEXPR ||
          function.getType() == HiveParser.TOK_SUBQUERY_EXPR) {
        function = (ASTNode)function.getChild(0);
      }
      doPhase1GetAllAggregations(function, aggregationTrees, wdwFns);
    }

    // window based aggregations are handled differently
    for (ASTNode wdwFn : wdwFns) {
      WindowingSpec spec = qb.getWindowingSpec(dest);
      if(spec == null) {
        queryProperties.setHasWindowing(true);
        spec = new WindowingSpec();
        qb.addDestToWindowingSpec(dest, spec);
      }
      HashMap<String, ASTNode> wExprsInDest = qb.getParseInfo().getWindowingExprsForClause(dest);
      int wColIdx = spec.getWindowExpressions() == null ? 0 : spec.getWindowExpressions().size();
      WindowFunctionSpec wFnSpec = processWindowFunction(wdwFn,
        (ASTNode)wdwFn.getChild(wdwFn.getChildCount()-1));
      // If this is a duplicate invocation of a function; don't add to WindowingSpec.
      if ( wExprsInDest != null &&
          wExprsInDest.containsKey(wFnSpec.getExpression().toStringTree())) {
        continue;
      }
      wFnSpec.setAlias("_wcol" + wColIdx);
      spec.addWindowFunction(wFnSpec);
      qb.getParseInfo().addWindowingExprToClause(dest, wFnSpec.getExpression());
    }

    return aggregationTrees;
  }

  private void doPhase1GetColumnAliasesFromSelect(
      ASTNode selectExpr, QBParseInfo qbp) {
    for (int i = 0; i < selectExpr.getChildCount(); ++i) {
      ASTNode selExpr = (ASTNode) selectExpr.getChild(i);
      if ((selExpr.getToken().getType() == HiveParser.TOK_SELEXPR)
          && (selExpr.getChildCount() == 2)) {
        String columnAlias = unescapeIdentifier(selExpr.getChild(1).getText());
        qbp.setExprToColumnAlias((ASTNode) selExpr.getChild(0), columnAlias);
      }
    }
  }

  /**
   * DFS-scan the expressionTree to find all aggregation subtrees and put them
   * in aggregations.
   *
   * @param expressionTree
   * @param aggregations
   *          the key to the HashTable is the toStringTree() representation of
   *          the aggregation subtree.
   * @throws SemanticException
   */
  private void doPhase1GetAllAggregations(ASTNode expressionTree,
      HashMap<String, ASTNode> aggregations, List<ASTNode> wdwFns) throws SemanticException {
    int exprTokenType = expressionTree.getToken().getType();
    if (exprTokenType == HiveParser.TOK_FUNCTION
        || exprTokenType == HiveParser.TOK_FUNCTIONDI
        || exprTokenType == HiveParser.TOK_FUNCTIONSTAR) {
      assert (expressionTree.getChildCount() != 0);
      if (expressionTree.getChild(expressionTree.getChildCount()-1).getType()
          == HiveParser.TOK_WINDOWSPEC) {
        wdwFns.add(expressionTree);
        return;
      }
      if (expressionTree.getChild(0).getType() == HiveParser.Identifier) {
        String functionName = unescapeIdentifier(expressionTree.getChild(0)
            .getText());
        if(FunctionRegistry.impliesOrder(functionName)) {
          throw new SemanticException(ErrorMsg.MISSING_OVER_CLAUSE.getMsg(functionName));
        }
        if (FunctionRegistry.getGenericUDAFResolver(functionName) != null) {
          if(containsLeadLagUDF(expressionTree)) {
            throw new SemanticException(ErrorMsg.MISSING_OVER_CLAUSE.getMsg(functionName));
          }
          aggregations.put(expressionTree.toStringTree().toLowerCase(), expressionTree);
          FunctionInfo fi = FunctionRegistry.getFunctionInfo(functionName);
          if (!fi.isNative()) {
            unparseTranslator.addIdentifierTranslation((ASTNode) expressionTree
                .getChild(0));
          }
          return;
        }
      }
    }
    for (int i = 0; i < expressionTree.getChildCount(); i++) {
      doPhase1GetAllAggregations((ASTNode) expressionTree.getChild(i),
          aggregations, wdwFns);
    }
  }

  private List<ASTNode> doPhase1GetDistinctFuncExprs(
      HashMap<String, ASTNode> aggregationTrees) throws SemanticException {
    List<ASTNode> exprs = new ArrayList<ASTNode>();
    for (Map.Entry<String, ASTNode> entry : aggregationTrees.entrySet()) {
      ASTNode value = entry.getValue();
      assert (value != null);
      if (value.getToken().getType() == HiveParser.TOK_FUNCTIONDI) {
        exprs.add(value);
      }
    }
    return exprs;
  }

  public static String generateErrorMessage(ASTNode ast, String message) {
    StringBuilder sb = new StringBuilder();
    if (ast == null) {
      sb.append("The abstract syntax tree is null");
      return sb.toString();
    }
    sb.append(ast.getLine());
    sb.append(":");
    sb.append(ast.getCharPositionInLine());
    sb.append(" ");
    sb.append(message);
    sb.append(". Error encountered near token '");
    sb.append(ErrorMsg.getText(ast));
    sb.append("'");
    return sb.toString();
  }

  ASTNode getAST() {
    return this.ast;
  }

  protected void setAST(ASTNode newAST) {
    this.ast = newAST;
  }

  /**
   * Goes though the tabref tree and finds the alias for the table. Once found,
   * it records the table name-> alias association in aliasToTabs. It also makes
   * an association from the alias to the table AST in parse info.
   *
   * @return the alias of the table
   */
  private String processTable(QB qb, ASTNode tabref) throws SemanticException {
    // For each table reference get the table name
    // and the alias (if alias is not present, the table name
    // is used as an alias)
    int aliasIndex = 0;
    int propsIndex = -1;
    int tsampleIndex = -1;
    int ssampleIndex = -1;
    for (int index = 1; index < tabref.getChildCount(); index++) {
      ASTNode ct = (ASTNode) tabref.getChild(index);
      if (ct.getToken().getType() == HiveParser.TOK_TABLEBUCKETSAMPLE) {
        tsampleIndex = index;
      } else if (ct.getToken().getType() == HiveParser.TOK_TABLESPLITSAMPLE) {
        ssampleIndex = index;
      } else if (ct.getToken().getType() == HiveParser.TOK_TABLEPROPERTIES) {
        propsIndex = index;
      } else {
        aliasIndex = index;
      }
    }

    ASTNode tableTree = (ASTNode) (tabref.getChild(0));

    String tabIdName = getUnescapedName(tableTree);

    String alias;
    if (aliasIndex != 0) {
      alias = unescapeIdentifier(tabref.getChild(aliasIndex).getText());
    }
    else {
      alias = getUnescapedUnqualifiedTableName(tableTree);
    }

    if (propsIndex >= 0) {
      Tree propsAST = tabref.getChild(propsIndex);
      Map<String, String> props = DDLSemanticAnalyzer.getProps((ASTNode) propsAST.getChild(0));
      qb.setTabProps(alias, props);
    }

    // If the alias is already there then we have a conflict
    if (qb.exists(alias)) {
      throw new SemanticException(ErrorMsg.AMBIGUOUS_TABLE_ALIAS.getMsg(tabref
          .getChild(aliasIndex)));
    }
    if (tsampleIndex >= 0) {
      ASTNode sampleClause = (ASTNode) tabref.getChild(tsampleIndex);
      ArrayList<ASTNode> sampleCols = new ArrayList<ASTNode>();
      if (sampleClause.getChildCount() > 2) {
        for (int i = 2; i < sampleClause.getChildCount(); i++) {
          sampleCols.add((ASTNode) sampleClause.getChild(i));
        }
      }
      // TODO: For now only support sampling on up to two columns
      // Need to change it to list of columns
      if (sampleCols.size() > 2) {
        throw new SemanticException(generateErrorMessage(
            (ASTNode) tabref.getChild(0),
            ErrorMsg.SAMPLE_RESTRICTION.getMsg()));
      }
      qb.getParseInfo().setTabSample(
          alias,
          new TableSample(
              unescapeIdentifier(sampleClause.getChild(0).getText()),
              unescapeIdentifier(sampleClause.getChild(1).getText()),
              sampleCols));
      if (unparseTranslator.isEnabled()) {
        for (ASTNode sampleCol : sampleCols) {
          unparseTranslator.addIdentifierTranslation((ASTNode) sampleCol
              .getChild(0));
        }
      }
    } else if (ssampleIndex >= 0) {
      ASTNode sampleClause = (ASTNode) tabref.getChild(ssampleIndex);

      Tree type = sampleClause.getChild(0);
      Tree numerator = sampleClause.getChild(1);
      String value = unescapeIdentifier(numerator.getText());


      SplitSample sample;
      if (type.getType() == HiveParser.TOK_PERCENT) {
        assertCombineInputFormat(numerator, "Percentage");
        Double percent = Double.valueOf(value).doubleValue();
        if (percent < 0  || percent > 100) {
          throw new SemanticException(generateErrorMessage((ASTNode) numerator,
              "Sampling percentage should be between 0 and 100"));
        }
        int seedNum = conf.getIntVar(ConfVars.HIVESAMPLERANDOMNUM);
        sample = new SplitSample(percent, seedNum);
      } else if (type.getType() == HiveParser.TOK_ROWCOUNT) {
        sample = new SplitSample(Integer.valueOf(value));
      } else {
        assert type.getType() == HiveParser.TOK_LENGTH;
        assertCombineInputFormat(numerator, "Total Length");
        long length = Integer.valueOf(value.substring(0, value.length() - 1));
        char last = value.charAt(value.length() - 1);
        if (last == 'k' || last == 'K') {
          length <<= 10;
        } else if (last == 'm' || last == 'M') {
          length <<= 20;
        } else if (last == 'g' || last == 'G') {
          length <<= 30;
        }
        int seedNum = conf.getIntVar(ConfVars.HIVESAMPLERANDOMNUM);
        sample = new SplitSample(length, seedNum);
      }
      String alias_id = getAliasId(alias, qb);
      nameToSplitSample.put(alias_id, sample);
    }
    // Insert this map into the stats
    qb.setTabAlias(alias, tabIdName);
    qb.addAlias(alias);

    qb.getParseInfo().setSrcForAlias(alias, tableTree);

    unparseTranslator.addTableNameTranslation(tableTree, SessionState.get().getCurrentDatabase());
    if (aliasIndex != 0) {
      unparseTranslator.addIdentifierTranslation((ASTNode) tabref
          .getChild(aliasIndex));
    }

    return alias;
  }

  Map<String, SplitSample> getNameToSplitSampleMap() {
    return this.nameToSplitSample;
  }

  // Generate a temp table out of a value clause
  private ASTNode genValuesTempTable(ASTNode originalFrom) throws SemanticException {
    // Pick a name for the table
    SessionState ss = SessionState.get();
    String tableName = VALUES_TMP_TABLE_NAME_PREFIX + ss.getNextValuesTempTableSuffix();

    // Step 1, parse the values clause we were handed
    List<? extends Node> fromChildren = originalFrom.getChildren();
    // First child should be the virtual table ref
    ASTNode virtualTableRef = (ASTNode)fromChildren.get(0);
    assert virtualTableRef.getToken().getType() == HiveParser.TOK_VIRTUAL_TABREF :
        "Expected first child of TOK_VIRTUAL_TABLE to be TOK_VIRTUAL_TABREF but was " +
            virtualTableRef.getName();

    List<? extends Node> virtualTableRefChildren = virtualTableRef.getChildren();
    // First child of this should be the table name.  If it's anonymous,
    // then we don't have a table name.
    ASTNode tabName = (ASTNode)virtualTableRefChildren.get(0);
    if (tabName.getToken().getType() != HiveParser.TOK_ANONYMOUS) {
      // TODO, if you want to make select ... from (values(...) as foo(...) work,
      // you need to parse this list of columns names and build it into the table
      throw new SemanticException(ErrorMsg.VALUES_TABLE_CONSTRUCTOR_NOT_SUPPORTED.getMsg());
    }

    // The second child of the TOK_VIRTUAL_TABLE should be TOK_VALUES_TABLE
    ASTNode valuesTable = (ASTNode)fromChildren.get(1);
    assert valuesTable.getToken().getType() == HiveParser.TOK_VALUES_TABLE :
        "Expected second child of TOK_VIRTUAL_TABLE to be TOK_VALUE_TABLE but was " +
            valuesTable.getName();
    // Each of the children of TOK_VALUES_TABLE will be a TOK_VALUE_ROW
    List<? extends Node> valuesTableChildren = valuesTable.getChildren();

    // Now that we're going to start reading through the rows, open a file to write the rows too
    // If we leave this method before creating the temporary table we need to be sure to clean up
    // this file.
    Path tablePath = null;
    FileSystem fs = null;
    try {
      tablePath = Warehouse.getDnsPath(new Path(ss.getTempTableSpace(), tableName), conf);
      fs = tablePath.getFileSystem(conf);
      fs.mkdirs(tablePath);
      Path dataFile = new Path(tablePath, "data_file");
      FSDataOutputStream out = fs.create(dataFile);
      List<FieldSchema> fields = new ArrayList<FieldSchema>();

      boolean firstRow = true;
      for (Node n : valuesTableChildren) {
        ASTNode valuesRow = (ASTNode) n;
        assert valuesRow.getToken().getType() == HiveParser.TOK_VALUE_ROW :
            "Expected child of TOK_VALUE_TABLE to be TOK_VALUE_ROW but was " + valuesRow.getName();
        // Each of the children of this should be a literal
        List<? extends Node> valuesRowChildren = valuesRow.getChildren();
        boolean isFirst = true;
        int nextColNum = 1;
        for (Node n1 : valuesRowChildren) {
          ASTNode value = (ASTNode) n1;
          if (firstRow) {
            fields.add(new FieldSchema("tmp_values_col" + nextColNum++, "string", ""));
          }
          if (isFirst) isFirst = false;
          else out.writeBytes("\u0001");
          out.writeBytes(unparseExprForValuesClause(value));
        }
        out.writeBytes("\n");
        firstRow = false;
      }
      out.close();

      // Step 2, create a temp table, using the created file as the data
      StorageFormat format = new StorageFormat(conf);
      format.processStorageFormat("TextFile");
      Table table = db.newTable(tableName);
      table.setSerializationLib(format.getSerde());
      table.setFields(fields);
      table.setDataLocation(tablePath);
      table.getTTable().setTemporary(true);
      table.setStoredAsSubDirectories(false);
      table.setInputFormatClass(format.getInputFormat());
      table.setOutputFormatClass(format.getOutputFormat());
      db.createTable(table, false);
    } catch (Exception e) {
      String errMsg = ErrorMsg.INSERT_CANNOT_CREATE_TEMP_FILE.getMsg() + e.getMessage();
      LOG.error(errMsg);
      // Try to delete the file
      if (fs != null && tablePath != null) {
        try {
          fs.delete(tablePath, false);
        } catch (IOException swallowIt) {}
      }
      throw new SemanticException(errMsg, e);
    }

    // Step 3, return a new subtree with a from clause built around that temp table
    // The form of the tree is TOK_TABREF->TOK_TABNAME->identifier(tablename)
    Token t = new ClassicToken(HiveParser.TOK_TABREF);
    ASTNode tabRef = new ASTNode(t);
    t = new ClassicToken(HiveParser.TOK_TABNAME);
    ASTNode tabNameNode = new ASTNode(t);
    tabRef.addChild(tabNameNode);
    t = new ClassicToken(HiveParser.Identifier, tableName);
    ASTNode identifier = new ASTNode(t);
    tabNameNode.addChild(identifier);
    return tabRef;
  }

  // Take an expression in the values clause and turn it back into a string.  This is far from
  // comprehensive.  At the moment it only supports:
  // * literals (all types)
  // * unary negatives
  // * true/false
  private String unparseExprForValuesClause(ASTNode expr) throws SemanticException {
    switch (expr.getToken().getType()) {
      case HiveParser.Number:
        return expr.getText();

      case HiveParser.StringLiteral:
        return PlanUtils.stripQuotes(expr.getText());

      case HiveParser.KW_FALSE:
        // UDFToBoolean casts any non-empty string to true, so set this to false
        return "";

      case HiveParser.KW_TRUE:
        return "TRUE";

      case HiveParser.MINUS:
        return "-" + unparseExprForValuesClause((ASTNode)expr.getChildren().get(0));

      case HiveParser.TOK_NULL:
        // Hive's text input will translate this as a null
        return "\\N";

      default:
        throw new SemanticException("Expression of type " + expr.getText() +
            " not supported in insert/values");
    }

  }

  private void assertCombineInputFormat(Tree numerator, String message) throws SemanticException {
    String inputFormat = conf.getVar(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE).equals("tez") ?
      HiveConf.getVar(conf, HiveConf.ConfVars.HIVETEZINPUTFORMAT):
      HiveConf.getVar(conf, HiveConf.ConfVars.HIVEINPUTFORMAT);
    if (!inputFormat.equals(CombineHiveInputFormat.class.getName())) {
      throw new SemanticException(generateErrorMessage((ASTNode) numerator,
          message + " sampling is not supported in " + inputFormat));
    }
  }

  private String processSubQuery(QB qb, ASTNode subq) throws SemanticException {

    // This is a subquery and must have an alias
    if (subq.getChildCount() != 2) {
      throw new SemanticException(ErrorMsg.NO_SUBQUERY_ALIAS.getMsg(subq));
    }
    ASTNode subqref = (ASTNode) subq.getChild(0);
    String alias = unescapeIdentifier(subq.getChild(1).getText());

    // Recursively do the first phase of semantic analysis for the subquery
    QBExpr qbexpr = new QBExpr(alias);

    doPhase1QBExpr(subqref, qbexpr, qb.getId(), alias);

    // If the alias is already there then we have a conflict
    if (qb.exists(alias)) {
      throw new SemanticException(ErrorMsg.AMBIGUOUS_TABLE_ALIAS.getMsg(subq
          .getChild(1)));
    }
    // Insert this map into the stats
    qb.setSubqAlias(alias, qbexpr);
    qb.addAlias(alias);

    unparseTranslator.addIdentifierTranslation((ASTNode) subq.getChild(1));

    return alias;
  }

  /*
   * Phase1: hold onto any CTE definitions in aliasToCTE.
   * CTE definitions are global to the Query.
   */
  private void processCTE(QB qb, ASTNode ctes) throws SemanticException {

    int numCTEs = ctes.getChildCount();

    for(int i=0; i <numCTEs; i++) {
      ASTNode cte = (ASTNode) ctes.getChild(i);
      ASTNode cteQry = (ASTNode) cte.getChild(0);
      String alias = unescapeIdentifier(cte.getChild(1).getText());

      String qName = qb.getId() == null ? "" : qb.getId() + ":";
      qName += alias.toLowerCase();

      if ( aliasToCTEs.containsKey(qName)) {
        throw new SemanticException(ErrorMsg.AMBIGUOUS_TABLE_ALIAS.getMsg(cte.getChild(1)));
      }
      aliasToCTEs.put(qName, cteQry);
    }
  }

  /*
   * We allow CTE definitions in views. So we can end up with a hierarchy of CTE definitions:
   * - at the top level of a query statement
   * - where a view is referenced.
   * - views may refer to other views.
   *
   * The scoping rules we use are: to search for a CTE from the current QB outwards. In order to
   * disambiguate between CTES are different levels we qualify(prefix) them with the id of the QB
   * they appear in when adding them to the <code>aliasToCTEs</code> map.
   *
   */
  private ASTNode findCTEFromName(QB qb, String cteName) {

    /*
     * When saving a view definition all table references in the AST are qualified; including CTE references.
     * Where as CTE definitions have no DB qualifier; so we strip out the DB qualifier before searching in
     * <code>aliasToCTEs</code> map.
     */
    String currDB = SessionState.get().getCurrentDatabase();
    if ( currDB != null && cteName.startsWith(currDB) &&
        cteName.length() > currDB.length() &&
        cteName.charAt(currDB.length()) == '.'   ) {
      cteName = cteName.substring(currDB.length() + 1);
    }

    StringBuffer qId = new StringBuffer();
    if (qb.getId() != null) {
      qId.append(qb.getId());
    }

    while (qId.length() > 0) {
      String nm = qId + ":" + cteName;
      if (aliasToCTEs.containsKey(nm)) {
        return aliasToCTEs.get(nm);
      }
      int lastIndex = qId.lastIndexOf(":");
      lastIndex = lastIndex < 0 ? 0 : lastIndex;
      qId.setLength(lastIndex);
    }
    return aliasToCTEs.get(cteName);
  }

  /*
   * If a CTE is referenced in a QueryBlock:
   * - add it as a SubQuery for now.
   *   - SQ.alias is the alias used in QB. (if no alias is specified,
   *     it used the CTE name. Works just like table references)
   *   - Adding SQ done by:
   *     - copying AST of CTE
   *     - setting ASTOrigin on cloned AST.
   *   - trigger phase 1 on new QBExpr.
   *   - update QB data structs: remove this as a table reference, move it to a SQ invocation.
   */
  private void addCTEAsSubQuery(QB qb, String cteName, String cteAlias) throws SemanticException {
    cteAlias = cteAlias == null ? cteName : cteAlias;
    ASTNode cteQryNode = findCTEFromName(qb, cteName);
    QBExpr cteQBExpr = new QBExpr(cteAlias);

    String cteText = ctx.getTokenRewriteStream().toString(
        cteQryNode.getTokenStartIndex(), cteQryNode.getTokenStopIndex());
    final ASTNodeOrigin cteOrigin = new ASTNodeOrigin("CTE", cteName,
        cteText, cteAlias, cteQryNode);
    cteQryNode = (ASTNode) ParseDriver.adaptor.dupTree(cteQryNode);
    SubQueryUtils.setOriginDeep(cteQryNode, cteOrigin);

    doPhase1QBExpr(cteQryNode, cteQBExpr, qb.getId(), cteAlias);
    qb.rewriteCTEToSubq(cteAlias, cteName, cteQBExpr);
  }

  static boolean isJoinToken(ASTNode node) {
    if ((node.getToken().getType() == HiveParser.TOK_JOIN)
        || (node.getToken().getType() == HiveParser.TOK_CROSSJOIN)
        || isOuterJoinToken(node)
        || (node.getToken().getType() == HiveParser.TOK_LEFTSEMIJOIN)
        || (node.getToken().getType() == HiveParser.TOK_UNIQUEJOIN)) {
      return true;
    }

    return false;
  }

  static private boolean isOuterJoinToken(ASTNode node) {
    return (node.getToken().getType() == HiveParser.TOK_LEFTOUTERJOIN)
      || (node.getToken().getType() == HiveParser.TOK_RIGHTOUTERJOIN)
      || (node.getToken().getType() == HiveParser.TOK_FULLOUTERJOIN);
  }

  /**
   * Given the AST with TOK_JOIN as the root, get all the aliases for the tables
   * or subqueries in the join.
   *
   * @param qb
   * @param join
   * @throws SemanticException
   */
  @SuppressWarnings("nls")
  private void processJoin(QB qb, ASTNode join) throws SemanticException {
    int numChildren = join.getChildCount();
    if ((numChildren != 2) && (numChildren != 3)
        && join.getToken().getType() != HiveParser.TOK_UNIQUEJOIN) {
      throw new SemanticException(generateErrorMessage(join,
          "Join with multiple children"));
    }

    queryProperties.incrementJoinCount(isOuterJoinToken(join));
    for (int num = 0; num < numChildren; num++) {
      ASTNode child = (ASTNode) join.getChild(num);
      if (child.getToken().getType() == HiveParser.TOK_TABREF) {
        processTable(qb, child);
      } else if (child.getToken().getType() == HiveParser.TOK_SUBQUERY) {
        processSubQuery(qb, child);
      } else if (child.getToken().getType() == HiveParser.TOK_PTBLFUNCTION) {
        queryProperties.setHasPTF(true);
        processPTF(qb, child);
        PTFInvocationSpec ptfInvocationSpec = qb.getPTFInvocationSpec(child);
        String inputAlias = ptfInvocationSpec == null ? null :
          ptfInvocationSpec.getFunction().getAlias();;
        if ( inputAlias == null ) {
          throw new SemanticException(generateErrorMessage(child,
              "PTF invocation in a Join must have an alias"));
        }

      } else if (child.getToken().getType() == HiveParser.TOK_LATERAL_VIEW ||
          child.getToken().getType() == HiveParser.TOK_LATERAL_VIEW_OUTER) {
        // SELECT * FROM src1 LATERAL VIEW udtf() AS myTable JOIN src2 ...
        // is not supported. Instead, the lateral view must be in a subquery
        // SELECT * FROM (SELECT * FROM src1 LATERAL VIEW udtf() AS myTable) a
        // JOIN src2 ...
        throw new SemanticException(ErrorMsg.LATERAL_VIEW_WITH_JOIN
            .getMsg(join));
      } else if (isJoinToken(child)) {
        processJoin(qb, child);
      }
    }
  }

  /**
   * Given the AST with TOK_LATERAL_VIEW as the root, get the alias for the
   * table or subquery in the lateral view and also make a mapping from the
   * alias to all the lateral view AST's.
   *
   * @param qb
   * @param lateralView
   * @return the alias for the table/subquery
   * @throws SemanticException
   */

  private String processLateralView(QB qb, ASTNode lateralView)
      throws SemanticException {
    int numChildren = lateralView.getChildCount();

    assert (numChildren == 2);
    ASTNode next = (ASTNode) lateralView.getChild(1);

    String alias = null;

    switch (next.getToken().getType()) {
    case HiveParser.TOK_TABREF:
      alias = processTable(qb, next);
      break;
    case HiveParser.TOK_SUBQUERY:
      alias = processSubQuery(qb, next);
      break;
    case HiveParser.TOK_LATERAL_VIEW:
    case HiveParser.TOK_LATERAL_VIEW_OUTER:
      alias = processLateralView(qb, next);
      break;
    default:
      throw new SemanticException(ErrorMsg.LATERAL_VIEW_INVALID_CHILD
          .getMsg(lateralView));
    }
    alias = alias.toLowerCase();
    qb.getParseInfo().addLateralViewForAlias(alias, lateralView);
    qb.addAlias(alias);
    return alias;
  }

  /**
   * Phase 1: (including, but not limited to):
   *
   * 1. Gets all the aliases for all the tables / subqueries and makes the
   * appropriate mapping in aliasToTabs, aliasToSubq 2. Gets the location of the
   * destination and names the clause "inclause" + i 3. Creates a map from a
   * string representation of an aggregation tree to the actual aggregation AST
   * 4. Creates a mapping from the clause name to the select expression AST in
   * destToSelExpr 5. Creates a mapping from a table alias to the lateral view
   * AST's in aliasToLateralViews
   *
   * @param ast
   * @param qb
   * @param ctx_1
   * @throws SemanticException
   */
  @SuppressWarnings({"fallthrough", "nls"})
  public boolean doPhase1(ASTNode ast, QB qb, Phase1Ctx ctx_1, PlannerContext plannerCtx)
      throws SemanticException {

    boolean phase1Result = true;
    QBParseInfo qbp = qb.getParseInfo();
    boolean skipRecursion = false;

    if (ast.getToken() != null) {
      skipRecursion = true;
      switch (ast.getToken().getType()) {
      case HiveParser.TOK_SELECTDI:
        qb.countSelDi();
        // fall through
      case HiveParser.TOK_SELECT:
        qb.countSel();
        qbp.setSelExprForClause(ctx_1.dest, ast);

        int posn = 0;
        if (((ASTNode) ast.getChild(0)).getToken().getType() == HiveParser.TOK_HINTLIST) {
          qbp.setHints((ASTNode) ast.getChild(0));
          posn++;
        }

        if ((ast.getChild(posn).getChild(0).getType() == HiveParser.TOK_TRANSFORM))
          queryProperties.setUsesScript(true);

        LinkedHashMap<String, ASTNode> aggregations = doPhase1GetAggregationsFromSelect(ast,
            qb, ctx_1.dest);
        doPhase1GetColumnAliasesFromSelect(ast, qbp);
        qbp.setAggregationExprsForClause(ctx_1.dest, aggregations);
        qbp.setDistinctFuncExprsForClause(ctx_1.dest,
        doPhase1GetDistinctFuncExprs(aggregations));
        break;

      case HiveParser.TOK_WHERE:
        qbp.setWhrExprForClause(ctx_1.dest, ast);
        if (!SubQueryUtils.findSubQueries((ASTNode) ast.getChild(0)).isEmpty())
            queryProperties.setFilterWithSubQuery(true);
        break;

      case HiveParser.TOK_INSERT_INTO:
        String currentDatabase = SessionState.get().getCurrentDatabase();
        String tab_name = getUnescapedName((ASTNode) ast.getChild(0).getChild(0), currentDatabase);
        qbp.addInsertIntoTable(tab_name);

      case HiveParser.TOK_DESTINATION:
        ctx_1.dest = "insclause-" + ctx_1.nextNum;
        ctx_1.nextNum++;
        boolean isTmpFileDest = false;
        if (ast.getChildCount() > 0 && ast.getChild(0) instanceof ASTNode) {
          ASTNode ch = (ASTNode)ast.getChild(0);
          if (ch.getToken().getType() == HiveParser.TOK_DIR
              && ch.getChildCount() > 0 && ch.getChild(0) instanceof ASTNode) {
            ch = (ASTNode)ch.getChild(0);
            isTmpFileDest = ch.getToken().getType() == HiveParser.TOK_TMP_FILE;
          }
        }

        // is there a insert in the subquery
        if (qbp.getIsSubQ() && !isTmpFileDest) {
          throw new SemanticException(ErrorMsg.NO_INSERT_INSUBQUERY.getMsg(ast));
        }

        if (plannerCtx != null) {
          plannerCtx.setInsertToken(ast, isTmpFileDest);
        }

        qbp.setDestForClause(ctx_1.dest, (ASTNode) ast.getChild(0));

        if (qbp.getClauseNamesForDest().size() > 1) {
          queryProperties.setMultiDestQuery(true);
        }
        break;

      case HiveParser.TOK_FROM:
        int child_count = ast.getChildCount();
        if (child_count != 1) {
          throw new SemanticException(generateErrorMessage(ast,
              "Multiple Children " + child_count));
        }

        // Check if this is a subquery / lateral view
        ASTNode frm = (ASTNode) ast.getChild(0);
        if (frm.getToken().getType() == HiveParser.TOK_TABREF) {
          processTable(qb, frm);
        } else if (frm.getToken().getType() == HiveParser.TOK_VIRTUAL_TABLE) {
          // Create a temp table with the passed values in it then rewrite this portion of the
          // tree to be from that table.
          ASTNode newFrom = genValuesTempTable(frm);
          ast.setChild(0, newFrom);
          processTable(qb, newFrom);
        } else if (frm.getToken().getType() == HiveParser.TOK_SUBQUERY) {
          processSubQuery(qb, frm);
        } else if (frm.getToken().getType() == HiveParser.TOK_LATERAL_VIEW ||
            frm.getToken().getType() == HiveParser.TOK_LATERAL_VIEW_OUTER) {
          queryProperties.setHasLateralViews(true);
          processLateralView(qb, frm);
        } else if (isJoinToken(frm)) {
          processJoin(qb, frm);
          qbp.setJoinExpr(frm);
        }else if(frm.getToken().getType() == HiveParser.TOK_PTBLFUNCTION){
          queryProperties.setHasPTF(true);
          processPTF(qb, frm);
        }
        break;

      case HiveParser.TOK_CLUSTERBY:
        // Get the clusterby aliases - these are aliased to the entries in the
        // select list
        queryProperties.setHasClusterBy(true);
        qbp.setClusterByExprForClause(ctx_1.dest, ast);
        break;

      case HiveParser.TOK_DISTRIBUTEBY:
        // Get the distribute by aliases - these are aliased to the entries in
        // the
        // select list
        queryProperties.setHasDistributeBy(true);
        qbp.setDistributeByExprForClause(ctx_1.dest, ast);
        if (qbp.getClusterByForClause(ctx_1.dest) != null) {
          throw new SemanticException(generateErrorMessage(ast,
              ErrorMsg.CLUSTERBY_DISTRIBUTEBY_CONFLICT.getMsg()));
        } else if (qbp.getOrderByForClause(ctx_1.dest) != null) {
          throw new SemanticException(generateErrorMessage(ast,
              ErrorMsg.ORDERBY_DISTRIBUTEBY_CONFLICT.getMsg()));
        }
        break;

      case HiveParser.TOK_SORTBY:
     // Get the sort by aliases - these are aliased to the entries in the
        // select list
        queryProperties.setHasSortBy(true);
        qbp.setSortByExprForClause(ctx_1.dest, ast);
        if (qbp.getClusterByForClause(ctx_1.dest) != null) {
          throw new SemanticException(generateErrorMessage(ast,
              ErrorMsg.CLUSTERBY_SORTBY_CONFLICT.getMsg()));
        } else if (qbp.getOrderByForClause(ctx_1.dest) != null) {
          throw new SemanticException(generateErrorMessage(ast,
              ErrorMsg.ORDERBY_SORTBY_CONFLICT.getMsg()));
        }

        break;

      case HiveParser.TOK_ORDERBY:
        // Get the order by aliases - these are aliased to the entries in the
        // select list
        queryProperties.setHasOrderBy(true);
        qbp.setOrderByExprForClause(ctx_1.dest, ast);
        if (qbp.getClusterByForClause(ctx_1.dest) != null) {
          throw new SemanticException(generateErrorMessage(ast,
              ErrorMsg.CLUSTERBY_ORDERBY_CONFLICT.getMsg()));
        }
        break;

      case HiveParser.TOK_GROUPBY:
      case HiveParser.TOK_ROLLUP_GROUPBY:
      case HiveParser.TOK_CUBE_GROUPBY:
      case HiveParser.TOK_GROUPING_SETS:
        // Get the groupby aliases - these are aliased to the entries in the
        // select list
        queryProperties.setHasGroupBy(true);
        if (qbp.getJoinExpr() != null) {
          queryProperties.setHasJoinFollowedByGroupBy(true);
        }
        if (qbp.getSelForClause(ctx_1.dest).getToken().getType() == HiveParser.TOK_SELECTDI) {
          throw new SemanticException(generateErrorMessage(ast,
              ErrorMsg.SELECT_DISTINCT_WITH_GROUPBY.getMsg()));
        }
        qbp.setGroupByExprForClause(ctx_1.dest, ast);
        skipRecursion = true;

        // Rollup and Cubes are syntactic sugar on top of grouping sets
        if (ast.getToken().getType() == HiveParser.TOK_ROLLUP_GROUPBY) {
          qbp.getDestRollups().add(ctx_1.dest);
        } else if (ast.getToken().getType() == HiveParser.TOK_CUBE_GROUPBY) {
          qbp.getDestCubes().add(ctx_1.dest);
        } else if (ast.getToken().getType() == HiveParser.TOK_GROUPING_SETS) {
          qbp.getDestGroupingSets().add(ctx_1.dest);
        }
        break;

      case HiveParser.TOK_HAVING:
        qbp.setHavingExprForClause(ctx_1.dest, ast);
        qbp.addAggregationExprsForClause(ctx_1.dest,
            doPhase1GetAggregationsFromSelect(ast, qb, ctx_1.dest));
        break;

      case HiveParser.KW_WINDOW:
        if (!qb.hasWindowingSpec(ctx_1.dest) ) {
          throw new SemanticException(generateErrorMessage(ast,
              "Query has no Cluster/Distribute By; but has a Window definition"));
        }
        handleQueryWindowClauses(qb, ctx_1, ast);
        break;

      case HiveParser.TOK_LIMIT:
        qbp.setDestLimit(ctx_1.dest, new Integer(ast.getChild(0).getText()));
        break;

      case HiveParser.TOK_ANALYZE:
        // Case of analyze command

        String table_name = getUnescapedName((ASTNode) ast.getChild(0).getChild(0));


        qb.setTabAlias(table_name, table_name);
        qb.addAlias(table_name);
        qb.getParseInfo().setIsAnalyzeCommand(true);
        qb.getParseInfo().setNoScanAnalyzeCommand(this.noscan);
        qb.getParseInfo().setPartialScanAnalyzeCommand(this.partialscan);
        // Allow analyze the whole table and dynamic partitions
        HiveConf.setVar(conf, HiveConf.ConfVars.DYNAMICPARTITIONINGMODE, "nonstrict");
        HiveConf.setVar(conf, HiveConf.ConfVars.HIVEMAPREDMODE, "nonstrict");

        break;

      case HiveParser.TOK_UNION:
        if (!qbp.getIsSubQ()) {
          // this shouldn't happen. The parser should have converted the union to be
          // contained in a subquery. Just in case, we keep the error as a fallback.
          throw new SemanticException(generateErrorMessage(ast,
              ErrorMsg.UNION_NOTIN_SUBQ.getMsg()));
        }
        skipRecursion = false;
        break;

      case HiveParser.TOK_INSERT:
        ASTNode destination = (ASTNode) ast.getChild(0);
        Tree tab = destination.getChild(0);

        // Proceed if AST contains partition & If Not Exists
        if (destination.getChildCount() == 2 &&
            tab.getChildCount() == 2 &&
            destination.getChild(1).getType() == HiveParser.TOK_IFNOTEXISTS) {
          String tableName = tab.getChild(0).getChild(0).getText();

          Tree partitions = tab.getChild(1);
          int childCount = partitions.getChildCount();
          HashMap<String, String> partition = new HashMap<String, String>();
          for (int i = 0; i < childCount; i++) {
            String partitionName = partitions.getChild(i).getChild(0).getText();
            Tree pvalue = partitions.getChild(i).getChild(1);
            if (pvalue == null) {
              break;
            }
            String partitionVal = stripQuotes(pvalue.getText());
            partition.put(partitionName, partitionVal);
          }
          // if it is a dynamic partition throw the exception
          if (childCount == partition.size()) {
            try {
              Table table = db.getTable(tableName);
              Partition parMetaData = db.getPartition(table, partition, false);
              // Check partition exists if it exists skip the overwrite
              if (parMetaData != null) {
                phase1Result = false;
                skipRecursion = true;
                LOG.info("Partition already exists so insert into overwrite " +
                    "skipped for partition : " + parMetaData.toString());
                break;
              }
            } catch (HiveException e) {
              LOG.info("Error while getting metadata : ", e);
            }
          } else {
            throw new SemanticException(ErrorMsg.INSERT_INTO_DYNAMICPARTITION_IFNOTEXISTS
                .getMsg(partition.toString()));
          }
        }
        skipRecursion = false;
        break;
      case HiveParser.TOK_LATERAL_VIEW:
      case HiveParser.TOK_LATERAL_VIEW_OUTER:
        // todo: nested LV
        assert ast.getChildCount() == 1;
        qb.getParseInfo().getDestToLateralView().put(ctx_1.dest, ast);
        break;
      case HiveParser.TOK_CTE:
        processCTE(qb, ast);
        break;
      default:
        skipRecursion = false;
        break;
      }
    }

    if (!skipRecursion) {
      // Iterate over the rest of the children
      int child_count = ast.getChildCount();
      for (int child_pos = 0; child_pos < child_count && phase1Result; ++child_pos) {
        // Recurse
        phase1Result = phase1Result && doPhase1(
            (ASTNode)ast.getChild(child_pos), qb, ctx_1, plannerCtx);
      }
    }
    return phase1Result;
  }

  private void getMetaData(QBExpr qbexpr, ReadEntity parentInput)
      throws SemanticException {
    if (qbexpr.getOpcode() == QBExpr.Opcode.NULLOP) {
      getMetaData(qbexpr.getQB(), parentInput);
    } else {
      getMetaData(qbexpr.getQBExpr1(), parentInput);
      getMetaData(qbexpr.getQBExpr2(), parentInput);
    }
  }

  public Table getTable(TableScanOperator ts) {
    return topToTable.get(ts);
  }

  public void getMetaData(QB qb) throws SemanticException {
    getMetaData(qb, null);
  }

  @SuppressWarnings("nls")
  public void getMetaData(QB qb, ReadEntity parentInput) throws SemanticException {
    try {

      LOG.info("Get metadata for source tables");

      // Go over the tables and populate the related structures.
      // We have to materialize the table alias list since we might
      // modify it in the middle for view rewrite.
      List<String> tabAliases = new ArrayList<String>(qb.getTabAliases());

      // Keep track of view alias to view name and read entity
      // For eg: for a query like 'select * from V3', where V3 -> V2, V2 -> V1, V1 -> T
      // keeps track of full view name and read entity corresponding to alias V3, V3:V2, V3:V2:V1.
      // This is needed for tracking the dependencies for inputs, along with their parents.
      Map<String, ObjectPair<String, ReadEntity>> aliasToViewInfo =
          new HashMap<String, ObjectPair<String, ReadEntity>>();

      /*
       * used to capture view to SQ conversions. This is used to check for
       * recursive CTE invocations.
       */
      Map<String, String> sqAliasToCTEName = new HashMap<String, String>();

      for (String alias : tabAliases) {
        String tab_name = qb.getTabNameForAlias(alias);
        Table tab = db.getTable(tab_name, false);
        if (tab == null) {
          /*
           * if this s a CTE reference:
           * Add its AST as a SubQuery to this QB.
           */
          ASTNode cteNode = findCTEFromName(qb, tab_name.toLowerCase());
          if ( cteNode != null ) {
            String cte_name = tab_name.toLowerCase();
            if (ctesExpanded.contains(cte_name)) {
              throw new SemanticException("Recursive cte " + tab_name +
                  " detected (cycle: " + StringUtils.join(ctesExpanded, " -> ") +
                  " -> " + tab_name + ").");
            }
            addCTEAsSubQuery(qb, cte_name, alias);
            sqAliasToCTEName.put(alias, cte_name);
            continue;
          }
          ASTNode src = qb.getParseInfo().getSrcForAlias(alias);
          if (null != src) {
            throw new SemanticException(ErrorMsg.INVALID_TABLE.getMsg(src));
          } else {
            throw new SemanticException(ErrorMsg.INVALID_TABLE.getMsg(alias));
          }

        }

        // Disallow INSERT INTO on bucketized tables
        boolean isAcid = isAcidTable(tab);
        if (qb.getParseInfo().isInsertIntoTable(tab.getDbName(), tab.getTableName()) &&
            tab.getNumBuckets() > 0 && !isAcid) {
          throw new SemanticException(ErrorMsg.INSERT_INTO_BUCKETIZED_TABLE.
              getMsg("Table: " + tab_name));
        }
        // Disallow update and delete on non-acid tables
        if ((updating() || deleting()) && !isAcid) {
          // isAcidTable above also checks for whether we are using an acid compliant
          // transaction manager.  But that has already been caught in
          // UpdateDeleteSemanticAnalyzer, so if we are updating or deleting and getting nonAcid
          // here, it means the table itself doesn't support it.
          throw new SemanticException(ErrorMsg.ACID_OP_ON_NONACID_TABLE, tab_name);
        }

        // We check offline of the table, as if people only select from an
        // non-existing partition of an offline table, the partition won't
        // be added to inputs and validate() won't have the information to
        // check the table's offline status.
        // TODO: Modify the code to remove the checking here and consolidate
        // it in validate()
        //
        if (tab.isOffline()) {
          throw new SemanticException(ErrorMsg.OFFLINE_TABLE_OR_PARTITION.
              getMsg("Table " + getUnescapedName(qb.getParseInfo().getSrcForAlias(alias))));
        }

        if (tab.isView()) {
          if (qb.getParseInfo().isAnalyzeCommand()) {
            throw new SemanticException(ErrorMsg.ANALYZE_VIEW.getMsg());
          }
          String fullViewName = tab.getDbName() + "." + tab.getTableName();
          // Prevent view cycles
          if (viewsExpanded.contains(fullViewName)) {
            throw new SemanticException("Recursive view " + fullViewName +
                " detected (cycle: " + StringUtils.join(viewsExpanded, " -> ") +
                " -> " + fullViewName + ").");
          }
          replaceViewReferenceWithDefinition(qb, tab, tab_name, alias);
          // This is the last time we'll see the Table objects for views, so add it to the inputs
          // now
          ReadEntity viewInput = new ReadEntity(tab, parentInput);
          viewInput = PlanUtils.addInput(inputs, viewInput);
          aliasToViewInfo.put(alias, new ObjectPair<String, ReadEntity>(fullViewName, viewInput));
          viewAliasToInput.put(getAliasId(alias, qb), viewInput);
          continue;
        }

        if (!InputFormat.class.isAssignableFrom(tab.getInputFormatClass())) {
          throw new SemanticException(generateErrorMessage(
              qb.getParseInfo().getSrcForAlias(alias),
              ErrorMsg.INVALID_INPUT_FORMAT_TYPE.getMsg()));
        }

        qb.getMetaData().setSrcForAlias(alias, tab);

        if (qb.getParseInfo().isAnalyzeCommand()) {
          // allow partial partition specification for nonscan since noscan is fast.
          tableSpec ts = new tableSpec(db, conf, (ASTNode) ast.getChild(0), true, this.noscan);
          if (ts.specType == SpecType.DYNAMIC_PARTITION) { // dynamic partitions
            try {
              ts.partitions = db.getPartitionsByNames(ts.tableHandle, ts.partSpec);
            } catch (HiveException e) {
              throw new SemanticException(generateErrorMessage(
                  qb.getParseInfo().getSrcForAlias(alias),
                  "Cannot get partitions for " + ts.partSpec), e);
            }
          }
          // validate partial scan command
          QBParseInfo qbpi = qb.getParseInfo();
          if (qbpi.isPartialScanAnalyzeCommand()) {
            Class<? extends InputFormat> inputFormatClass = null;
            switch (ts.specType) {
            case TABLE_ONLY:
            case DYNAMIC_PARTITION:
              inputFormatClass = ts.tableHandle.getInputFormatClass();
              break;
            case STATIC_PARTITION:
              inputFormatClass = ts.partHandle.getInputFormatClass();
              break;
            default:
              assert false;
            }
            // throw a HiveException for formats other than rcfile or orcfile.
            if (!(inputFormatClass.equals(RCFileInputFormat.class) || inputFormatClass
                .equals(OrcInputFormat.class))) {
              throw new SemanticException(ErrorMsg.ANALYZE_TABLE_PARTIALSCAN_NON_RCFILE.getMsg());
            }
          }

          qb.getParseInfo().addTableSpec(alias, ts);
        }

        ReadEntity parentViewInfo = PlanUtils.getParentViewInfo(getAliasId(alias, qb), viewAliasToInput);
        PlanUtils.addInput(inputs,
            new ReadEntity(tab, parentViewInfo, parentViewInfo == null));
      }

      LOG.info("Get metadata for subqueries");
      // Go over the subqueries and getMetaData for these
      for (String alias : qb.getSubqAliases()) {
        boolean wasView = aliasToViewInfo.containsKey(alias);
        boolean wasCTE = sqAliasToCTEName.containsKey(alias);
        ReadEntity newParentInput = null;
        if (wasView) {
          viewsExpanded.add(aliasToViewInfo.get(alias).getFirst());
          newParentInput = aliasToViewInfo.get(alias).getSecond();
        } else if (wasCTE) {
          ctesExpanded.add(sqAliasToCTEName.get(alias));
        }
        QBExpr qbexpr = qb.getSubqForAlias(alias);
        getMetaData(qbexpr, newParentInput);
        if (wasView) {
          viewsExpanded.remove(viewsExpanded.size() - 1);
        } else if (wasCTE) {
          ctesExpanded.remove(ctesExpanded.size() - 1);
        }
      }

      RowFormatParams rowFormatParams = new RowFormatParams();
      StorageFormat storageFormat = new StorageFormat(conf);

      LOG.info("Get metadata for destination tables");
      // Go over all the destination structures and populate the related
      // metadata
      QBParseInfo qbp = qb.getParseInfo();

      for (String name : qbp.getClauseNamesForDest()) {
        ASTNode ast = qbp.getDestForClause(name);
        switch (ast.getToken().getType()) {
        case HiveParser.TOK_TAB: {
          tableSpec ts = new tableSpec(db, conf, ast);
          if (ts.tableHandle.isView()) {
            throw new SemanticException(ErrorMsg.DML_AGAINST_VIEW.getMsg());
          }

          Class<?> outputFormatClass = ts.tableHandle.getOutputFormatClass();
          if (!ts.tableHandle.isNonNative() &&
              !HiveOutputFormat.class.isAssignableFrom(outputFormatClass)) {
            throw new SemanticException(ErrorMsg.INVALID_OUTPUT_FORMAT_TYPE
                .getMsg(ast, "The class is " + outputFormatClass.toString()));
          }

          // tableSpec ts is got from the query (user specified),
          // which means the user didn't specify partitions in their query,
          // but whether the table itself is partitioned is not know.
          if (ts.specType != SpecType.STATIC_PARTITION) {
            // This is a table or dynamic partition
            qb.getMetaData().setDestForAlias(name, ts.tableHandle);
            // has dynamic as well as static partitions
            if (ts.partSpec != null && ts.partSpec.size() > 0) {
              qb.getMetaData().setPartSpecForAlias(name, ts.partSpec);
            }
          } else {
            // This is a partition
            qb.getMetaData().setDestForAlias(name, ts.partHandle);
          }
          if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVESTATSAUTOGATHER)) {
            // Set that variable to automatically collect stats during the MapReduce job
            qb.getParseInfo().setIsInsertToTable(true);
            // Add the table spec for the destination table.
            qb.getParseInfo().addTableSpec(ts.tableName.toLowerCase(), ts);
          }
          break;
        }

        case HiveParser.TOK_LOCAL_DIR:
        case HiveParser.TOK_DIR: {
          // This is a dfs file
          String fname = stripQuotes(ast.getChild(0).getText());
          if ((!qb.getParseInfo().getIsSubQ())
              && (((ASTNode) ast.getChild(0)).getToken().getType() == HiveParser.TOK_TMP_FILE)) {

            if (qb.isCTAS()) {
              qb.setIsQuery(false);
              ctx.setResDir(null);
              ctx.setResFile(null);

              // allocate a temporary output dir on the location of the table
              String tableName = getUnescapedName((ASTNode) ast.getChild(0));
              String[] names = Utilities.getDbTableName(tableName);
              Path location;
              try {
                Warehouse wh = new Warehouse(conf);
                location = wh.getDatabasePath(db.getDatabase(names[0]));
              } catch (MetaException e) {
                throw new SemanticException(e);
              }
              try {
                fname = ctx.getExtTmpPathRelTo(
                    FileUtils.makeQualified(location, conf)).toString();
              } catch (Exception e) {
                throw new SemanticException(generateErrorMessage(ast,
                    "Error creating temporary folder on: " + location.toString()), e);
              }
              if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVESTATSAUTOGATHER)) {
                tableSpec ts = new tableSpec(db, conf, this.ast);
                // Set that variable to automatically collect stats during the MapReduce job
                qb.getParseInfo().setIsInsertToTable(true);
                // Add the table spec for the destination table.
                qb.getParseInfo().addTableSpec(ts.tableName.toLowerCase(), ts);
              }
            } else {
              // This is the only place where isQuery is set to true; it defaults to false.
              qb.setIsQuery(true);
              Path stagingPath = getStagingDirectoryPathname(qb);
              fname = stagingPath.toString();
              ctx.setResDir(stagingPath);
            }
          }
          qb.getMetaData().setDestForAlias(name, fname,
              (ast.getToken().getType() == HiveParser.TOK_DIR));

          CreateTableDesc localDirectoryDesc = new CreateTableDesc();
          boolean localDirectoryDescIsSet = false;
          int numCh = ast.getChildCount();
          for (int num = 1; num < numCh ; num++){
            ASTNode child = (ASTNode) ast.getChild(num);
            if (child != null) {
              if (storageFormat.fillStorageFormat(child)) {
                localDirectoryDesc.setOutputFormat(storageFormat.getOutputFormat());
                localDirectoryDesc.setSerName(storageFormat.getSerde());
                localDirectoryDescIsSet = true;
                continue;
              }
              switch (child.getToken().getType()) {
                case HiveParser.TOK_TABLEROWFORMAT:
                  rowFormatParams.analyzeRowFormat(child);
                  localDirectoryDesc.setFieldDelim(rowFormatParams.fieldDelim);
                  localDirectoryDesc.setLineDelim(rowFormatParams.lineDelim);
                  localDirectoryDesc.setCollItemDelim(rowFormatParams.collItemDelim);
                  localDirectoryDesc.setMapKeyDelim(rowFormatParams.mapKeyDelim);
                  localDirectoryDesc.setFieldEscape(rowFormatParams.fieldEscape);
                  localDirectoryDesc.setNullFormat(rowFormatParams.nullFormat);
                  localDirectoryDescIsSet=true;
                  break;
                case HiveParser.TOK_TABLESERIALIZER:
                  ASTNode serdeChild = (ASTNode) child.getChild(0);
                  storageFormat.setSerde(unescapeSQLString(serdeChild.getChild(0).getText()));
                  localDirectoryDesc.setSerName(storageFormat.getSerde());
                  localDirectoryDescIsSet=true;
                  break;
              }
            }
          }
          if (localDirectoryDescIsSet){
            qb.setLocalDirectoryDesc(localDirectoryDesc);
          }
          break;
        }
        default:
          throw new SemanticException(generateErrorMessage(ast,
              "Unknown Token Type " + ast.getToken().getType()));
        }
      }
    } catch (HiveException e) {
      // Has to use full name to make sure it does not conflict with
      // org.apache.commons.lang.StringUtils
      LOG.error(org.apache.hadoop.util.StringUtils.stringifyException(e));
      throw new SemanticException(e.getMessage(), e);
    }
  }

  /**
   * Checks if a given path is encrypted (valid only for HDFS files)
   * @param path The path to check for encryption
   * @return True if the path is encrypted; False if it is not encrypted
   * @throws HiveException If an error occurs while checking for encryption
   */
  private boolean isPathEncrypted(Path path) throws HiveException {
    HadoopShims.HdfsEncryptionShim hdfsEncryptionShim;

    hdfsEncryptionShim = SessionState.get().getHdfsEncryptionShim();
    if (hdfsEncryptionShim != null) {
      try {
        if (hdfsEncryptionShim.isPathEncrypted(path)) {
          return true;
        }
      } catch (Exception e) {
        throw new HiveException("Unable to determine if " + path + " is encrypted: " + e, e);
      }
    }

    return false;
  }

  /**
   * Compares to path key encryption strenghts.
   *
   * @param p1 Path to an HDFS file system
   * @param p2 Path to an HDFS file system
   * @return -1 if strength is weak; 0 if is equals; 1 if it is stronger
   * @throws HiveException If an error occurs while comparing key strengths.
   */
  private int comparePathKeyStrength(Path p1, Path p2) throws HiveException {
    HadoopShims.HdfsEncryptionShim hdfsEncryptionShim;

    hdfsEncryptionShim = SessionState.get().getHdfsEncryptionShim();
    if (hdfsEncryptionShim != null) {
      try {
        return hdfsEncryptionShim.comparePathKeyStrength(p1, p2);
      } catch (Exception e) {
        throw new HiveException("Unable to compare key strength for " + p1 + " and " + p2 + " : " + e, e);
      }
    }

    return 0; // Non-encrypted path (or equals strength)
  }

  /**
   * Checks if a given path has read-only access permissions.
   *
   * @param path The path to check for read-only permissions.
   * @return True if the path is read-only; False otherwise.
   * @throws HiveException If an error occurs while checking file permissions.
   */
  private boolean isPathReadOnly(Path path) throws HiveException {
    HiveConf conf = SessionState.get().getConf();
    try {
      FileSystem fs = path.getFileSystem(conf);
      UserGroupInformation ugi = Utils.getUGI();
      FileStatus status = fs.getFileStatus(path);

      // We just check for writing permissions. If it fails with AccessControException, then it
      // means the location may be read-only.
      FileUtils.checkFileAccessWithImpersonation(fs, status, FsAction.WRITE, ugi.getUserName());

      // Path has writing permissions
      return false;
    } catch (AccessControlException e) {
      // An AccessControlException may be caused for other different errors,
      // but we take it as if our path is read-only
      return true;
    } catch (Exception e) {
      throw new HiveException("Unable to determine if " + path + " is read only: " + e, e);
    }
  }

  /**
   * Gets the strongest encrypted table path.
   *
   * @param qb The QB object that contains a list of all table locations.
   * @return The strongest encrypted path. It may return NULL if there are not tables encrypted, or are not HDFS tables.
   * @throws HiveException if an error occurred attempting to compare the encryption strength
   */
  private Path getStrongestEncryptedTablePath(QB qb) throws HiveException {
    List<String> tabAliases = new ArrayList<String>(qb.getTabAliases());
    Path strongestPath = null;

    /* Walk through all found table locations to get the most encrypted table */
    for (String alias : tabAliases) {
      Table tab = qb.getMetaData().getTableForAlias(alias);
      if (tab != null) {
        Path tablePath = tab.getDataLocation();
        if (tablePath != null) {
          if ("hdfs".equalsIgnoreCase(tablePath.toUri().getScheme())) {
            if (isPathEncrypted(tablePath)) {
              if (strongestPath == null) {
                strongestPath = tablePath;
              } else if (comparePathKeyStrength(tablePath, strongestPath) > 0) {
                strongestPath = tablePath;
              }
            }
          }
        }
      }
    }

    return strongestPath;
  }

  /**
   * Gets the staging directory where MR files will be stored temporary.
   * It walks through the QB plan to find the correct location where save temporary files. This
   * temporary location (or staging directory) may be created inside encrypted tables locations for
   * security reasons. If the QB has read-only tables, then the older scratch directory will be used,
   * or a permission error will be thrown if the requested query table is encrypted and the old scratch
   * directory is not.
   *
   * @param qb The QB object that contains a list of all table locations.
   * @return The path to the staging directory.
   * @throws HiveException If an error occurs while identifying the correct staging location.
   */
  private Path getStagingDirectoryPathname(QB qb) throws HiveException {
    Path stagingPath = null, tablePath;

    // Looks for the most encrypted table location
    // It may return null if there are not tables encrypted, or are not part of HDFS
    tablePath = getStrongestEncryptedTablePath(qb);
    if (tablePath != null) {
      // At this point, tablePath is part of HDFS and it is encrypted
      if (isPathReadOnly(tablePath)) {
        Path tmpPath = ctx.getMRTmpPath();
        if (comparePathKeyStrength(tablePath, tmpPath) < 0) {
          throw new HiveException("Read-only encrypted tables cannot be read " +
              "if the scratch directory is not encrypted (or encryption is weak)");
        } else {
          stagingPath = tmpPath;
        }
      }

      if (stagingPath == null) {
        stagingPath = ctx.getMRTmpPath(tablePath.toUri());
      }
    } else {
      stagingPath = ctx.getMRTmpPath();
    }

    return stagingPath;
  }

  private void replaceViewReferenceWithDefinition(QB qb, Table tab,
      String tab_name, String alias) throws SemanticException {

    ParseDriver pd = new ParseDriver();
    ASTNode viewTree;
    final ASTNodeOrigin viewOrigin = new ASTNodeOrigin("VIEW", tab.getTableName(),
        tab.getViewExpandedText(), alias, qb.getParseInfo().getSrcForAlias(
            alias));
    try {
      String viewText = tab.getViewExpandedText();
      // Reparse text, passing null for context to avoid clobbering
      // the top-level token stream.
      ASTNode tree = pd.parse(viewText, ctx, false);
      tree = ParseUtils.findRootNonNullToken(tree);
      viewTree = tree;
      Dispatcher nodeOriginDispatcher = new Dispatcher() {
        @Override
        public Object dispatch(Node nd, java.util.Stack<Node> stack,
            Object... nodeOutputs) {
          ((ASTNode) nd).setOrigin(viewOrigin);
          return null;
        }
      };
      GraphWalker nodeOriginTagger = new DefaultGraphWalker(
          nodeOriginDispatcher);
      nodeOriginTagger.startWalking(java.util.Collections
          .<Node> singleton(viewTree), null);
    } catch (ParseException e) {
      // A user could encounter this if a stored view definition contains
      // an old SQL construct which has been eliminated in a later Hive
      // version, so we need to provide full debugging info to help
      // with fixing the view definition.
      LOG.error(org.apache.hadoop.util.StringUtils.stringifyException(e));
      StringBuilder sb = new StringBuilder();
      sb.append(e.getMessage());
      ErrorMsg.renderOrigin(sb, viewOrigin);
      throw new SemanticException(sb.toString(), e);
    }
    QBExpr qbexpr = new QBExpr(alias);
    doPhase1QBExpr(viewTree, qbexpr, qb.getId(), alias);
    qb.rewriteViewToSubq(alias, tab_name, qbexpr);
  }

  private boolean isPresent(String[] list, String elem) {
    for (String s : list) {
      if (s.toLowerCase().equals(elem)) {
        return true;
      }
    }

    return false;
  }

  /*
   * This method is invoked for unqualified column references in join conditions.
   * This is passed in the Alias to Operator mapping in the QueryBlock so far.
   * We try to resolve the unqualified column against each of the Operator Row Resolvers.
   * - if the column is present in only one RowResolver, we treat this as a reference to
   *   that Operator.
   * - if the column resolves with more than one RowResolver, we treat it as an Ambiguous
   *   reference.
   * - if the column doesn't resolve with any RowResolver, we treat this as an Invalid
   *   reference.
   */
  @SuppressWarnings("rawtypes")
  private String findAlias(ASTNode columnRef,
      Map<String, Operator> aliasToOpInfo) throws SemanticException {
    String colName = unescapeIdentifier(columnRef.getChild(0).getText()
        .toLowerCase());
    String tabAlias = null;
    if ( aliasToOpInfo != null ) {
      for (Map.Entry<String, Operator> opEntry : aliasToOpInfo.entrySet()) {
        Operator op = opEntry.getValue();
        RowResolver rr = opParseCtx.get(op).getRowResolver();
        ColumnInfo colInfo = rr.get(null, colName);
        if (colInfo != null) {
          if (tabAlias == null) {
            tabAlias = opEntry.getKey();
          } else {
            throw new SemanticException(
                ErrorMsg.AMBIGUOUS_TABLE_ALIAS.getMsg(columnRef.getChild(0)));
          }
        }
      }
    }
    if ( tabAlias == null ) {
      throw new SemanticException(ErrorMsg.INVALID_TABLE_ALIAS.getMsg(columnRef
          .getChild(0)));
    }
    return tabAlias;
  }

  @SuppressWarnings("nls")
  void parseJoinCondPopulateAlias(QBJoinTree joinTree, ASTNode condn,
      ArrayList<String> leftAliases, ArrayList<String> rightAliases,
      ArrayList<String> fields,
      Map<String, Operator> aliasToOpInfo) throws SemanticException {
    // String[] allAliases = joinTree.getAllAliases();
    switch (condn.getToken().getType()) {
    case HiveParser.TOK_TABLE_OR_COL:
      String tableOrCol = unescapeIdentifier(condn.getChild(0).getText()
          .toLowerCase());
      unparseTranslator.addIdentifierTranslation((ASTNode) condn.getChild(0));
      if (isPresent(joinTree.getLeftAliases(), tableOrCol)) {
        if (!leftAliases.contains(tableOrCol)) {
          leftAliases.add(tableOrCol);
        }
      } else if (isPresent(joinTree.getRightAliases(), tableOrCol)) {
        if (!rightAliases.contains(tableOrCol)) {
          rightAliases.add(tableOrCol);
        }
      } else {
        tableOrCol = findAlias(condn, aliasToOpInfo);
        if (isPresent(joinTree.getLeftAliases(), tableOrCol)) {
          if (!leftAliases.contains(tableOrCol)) {
            leftAliases.add(tableOrCol);
          }
        } else  {
          if (!rightAliases.contains(tableOrCol)) {
            rightAliases.add(tableOrCol);
          }
        }
      }
      break;

    case HiveParser.Identifier:
      // it may be a field name, return the identifier and let the caller decide
      // whether it is or not
      if (fields != null) {
        fields
            .add(unescapeIdentifier(condn.getToken().getText().toLowerCase()));
      }
      unparseTranslator.addIdentifierTranslation(condn);
      break;
    case HiveParser.Number:
    case HiveParser.StringLiteral:
    case HiveParser.BigintLiteral:
    case HiveParser.SmallintLiteral:
    case HiveParser.TinyintLiteral:
    case HiveParser.DecimalLiteral:
    case HiveParser.TOK_STRINGLITERALSEQUENCE:
    case HiveParser.TOK_CHARSETLITERAL:
    case HiveParser.KW_TRUE:
    case HiveParser.KW_FALSE:
      break;

    case HiveParser.TOK_FUNCTION:
      // check all the arguments
      for (int i = 1; i < condn.getChildCount(); i++) {
        parseJoinCondPopulateAlias(joinTree, (ASTNode) condn.getChild(i),
            leftAliases, rightAliases, null, aliasToOpInfo);
      }
      break;

    default:
      // This is an operator - so check whether it is unary or binary operator
      if (condn.getChildCount() == 1) {
        parseJoinCondPopulateAlias(joinTree, (ASTNode) condn.getChild(0),
            leftAliases, rightAliases, null, aliasToOpInfo);
      } else if (condn.getChildCount() == 2) {

        ArrayList<String> fields1 = null;
        // if it is a dot operator, remember the field name of the rhs of the
        // left semijoin
        if (joinTree.getNoSemiJoin() == false
            && condn.getToken().getType() == HiveParser.DOT) {
          // get the semijoin rhs table name and field name
          fields1 = new ArrayList<String>();
          int rhssize = rightAliases.size();
          parseJoinCondPopulateAlias(joinTree, (ASTNode) condn.getChild(0),
              leftAliases, rightAliases, null, aliasToOpInfo);
          String rhsAlias = null;

          if (rightAliases.size() > rhssize) { // the new table is rhs table
            rhsAlias = rightAliases.get(rightAliases.size() - 1);
          }

          parseJoinCondPopulateAlias(joinTree, (ASTNode) condn.getChild(1),
              leftAliases, rightAliases, fields1, aliasToOpInfo);
          if (rhsAlias != null && fields1.size() > 0) {
            joinTree.addRHSSemijoinColumns(rhsAlias, condn);
          }
        } else {
          parseJoinCondPopulateAlias(joinTree, (ASTNode) condn.getChild(0),
              leftAliases, rightAliases, null, aliasToOpInfo);
          parseJoinCondPopulateAlias(joinTree, (ASTNode) condn.getChild(1),
              leftAliases, rightAliases, fields1, aliasToOpInfo);
        }
      } else {
        throw new SemanticException(condn.toStringTree() + " encountered with "
            + condn.getChildCount() + " children");
      }
      break;
    }
  }

  private void populateAliases(List<String> leftAliases,
      List<String> rightAliases, ASTNode condn, QBJoinTree joinTree,
      List<String> leftSrc) throws SemanticException {
    if ((leftAliases.size() != 0) && (rightAliases.size() != 0)) {
      throw new SemanticException(ErrorMsg.INVALID_JOIN_CONDITION_1
          .getMsg(condn));
    }

    if (rightAliases.size() != 0) {
      assert rightAliases.size() == 1;
      joinTree.getExpressions().get(1).add(condn);
    } else if (leftAliases.size() != 0) {
      joinTree.getExpressions().get(0).add(condn);
      for (String s : leftAliases) {
        if (!leftSrc.contains(s)) {
          leftSrc.add(s);
        }
      }
    } else {
      throw new SemanticException(ErrorMsg.INVALID_JOIN_CONDITION_2
          .getMsg(condn));
    }
  }

  /*
   * refactored out of the Equality case of parseJoinCondition
   * so that this can be recursively called on its left tree in the case when
   * only left sources are referenced in a Predicate
   */
  void applyEqualityPredicateToQBJoinTree(QBJoinTree joinTree,
      JoinType type,
      List<String> leftSrc,
      ASTNode joinCond,
      ASTNode leftCondn,
      ASTNode rightCondn,
      List<String> leftCondAl1,
      List<String> leftCondAl2,
      List<String> rightCondAl1,
      List<String> rightCondAl2) throws SemanticException {
    if (leftCondAl1.size() != 0) {
      if ((rightCondAl1.size() != 0)
          || ((rightCondAl1.size() == 0) && (rightCondAl2.size() == 0))) {
        if (type.equals(JoinType.LEFTOUTER) ||
            type.equals(JoinType.FULLOUTER)) {
          if (conf.getBoolVar(HiveConf.ConfVars.HIVEOUTERJOINSUPPORTSFILTERS)) {
            joinTree.getFilters().get(0).add(joinCond);
          } else {
            LOG.warn(ErrorMsg.OUTERJOIN_USES_FILTERS);
            joinTree.getFiltersForPushing().get(0).add(joinCond);
          }
        } else {
          /*
           * If the rhs references table sources and this QBJoinTree has a leftTree;
           * hand it to the leftTree and let it recursively handle it.
           * There are 3 cases of passing a condition down:
           * 1. The leftSide && rightSide don't contains references to the leftTree's rightAlias
           *    => pass the lists down as is.
           * 2. The leftSide contains refs to the leftTree's rightAlias, the rightSide doesn't
           *    => switch the leftCondAl1 and leftConAl2 lists and pass down.
           * 3. The rightSide contains refs to the leftTree's rightAlias, the leftSide doesn't
           *    => switch the rightCondAl1 and rightConAl2 lists and pass down.
           * 4. In case both contain references to the leftTree's rightAlias
           *   => we cannot push the condition down.
           * 5. If either contain references to both left & right
           *    => we cannot push forward.
           */
          if (rightCondAl1.size() != 0) {
            QBJoinTree leftTree = joinTree.getJoinSrc();
            List<String> leftTreeLeftSrc = new ArrayList<String>();
            if (leftTree != null) {
              String leftTreeRightSource = leftTree.getRightAliases() != null &&
                  leftTree.getRightAliases().length > 0 ?
                  leftTree.getRightAliases()[0] : null;

              boolean leftHasRightReference = false;
              for (String r : leftCondAl1) {
                if (r.equals(leftTreeRightSource)) {
                  leftHasRightReference = true;
                  break;
                }
              }
              boolean rightHasRightReference = false;
              for (String r : rightCondAl1) {
                if (r.equals(leftTreeRightSource)) {
                  rightHasRightReference = true;
                  break;
                }
              }

              boolean pushedDown = false;
              if ( !leftHasRightReference && !rightHasRightReference ) {
                applyEqualityPredicateToQBJoinTree(leftTree, type, leftTreeLeftSrc,
                    joinCond, leftCondn, rightCondn,
                    leftCondAl1, leftCondAl2,
                    rightCondAl1, rightCondAl2);
                pushedDown = true;
              } else if ( !leftHasRightReference && rightHasRightReference && rightCondAl1.size() == 1 ) {
                applyEqualityPredicateToQBJoinTree(leftTree, type, leftTreeLeftSrc,
                    joinCond, leftCondn, rightCondn,
                    leftCondAl1, leftCondAl2,
                    rightCondAl2, rightCondAl1);
                pushedDown = true;
              } else if (leftHasRightReference && !rightHasRightReference && leftCondAl1.size() == 1 ) {
                applyEqualityPredicateToQBJoinTree(leftTree, type, leftTreeLeftSrc,
                    joinCond, leftCondn, rightCondn,
                    leftCondAl2, leftCondAl1,
                    rightCondAl1, rightCondAl2);
                pushedDown = true;
              }

              if (leftTreeLeftSrc.size() == 1) {
                leftTree.setLeftAlias(leftTreeLeftSrc.get(0));
              }
              if ( pushedDown) {
                return;
              }
            } // leftTree != null
          }
          joinTree.getFiltersForPushing().get(0).add(joinCond);
        }
      } else if (rightCondAl2.size() != 0) {
        populateAliases(leftCondAl1, leftCondAl2, leftCondn, joinTree,
            leftSrc);
        populateAliases(rightCondAl1, rightCondAl2, rightCondn, joinTree,
            leftSrc);
        boolean nullsafe = joinCond.getToken().getType() == HiveParser.EQUAL_NS;
        joinTree.getNullSafes().add(nullsafe);
      }
    } else if (leftCondAl2.size() != 0) {
      if ((rightCondAl2.size() != 0)
          || ((rightCondAl1.size() == 0) && (rightCondAl2.size() == 0))) {
        if (type.equals(JoinType.RIGHTOUTER)
            || type.equals(JoinType.FULLOUTER)) {
          if (conf.getBoolVar(HiveConf.ConfVars.HIVEOUTERJOINSUPPORTSFILTERS)) {
            joinTree.getFilters().get(1).add(joinCond);
          } else {
            LOG.warn(ErrorMsg.OUTERJOIN_USES_FILTERS);
            joinTree.getFiltersForPushing().get(1).add(joinCond);
          }
        } else {
          joinTree.getFiltersForPushing().get(1).add(joinCond);
        }
      } else if (rightCondAl1.size() != 0) {
        populateAliases(leftCondAl1, leftCondAl2, leftCondn, joinTree,
            leftSrc);
        populateAliases(rightCondAl1, rightCondAl2, rightCondn, joinTree,
            leftSrc);
        boolean nullsafe = joinCond.getToken().getType() == HiveParser.EQUAL_NS;
        joinTree.getNullSafes().add(nullsafe);
      }
    } else if (rightCondAl1.size() != 0) {
      if (type.equals(JoinType.LEFTOUTER)
          || type.equals(JoinType.FULLOUTER)) {
        if (conf.getBoolVar(HiveConf.ConfVars.HIVEOUTERJOINSUPPORTSFILTERS)) {
          joinTree.getFilters().get(0).add(joinCond);
        } else {
          LOG.warn(ErrorMsg.OUTERJOIN_USES_FILTERS);
          joinTree.getFiltersForPushing().get(0).add(joinCond);
        }
      } else {
        joinTree.getFiltersForPushing().get(0).add(joinCond);
      }
    } else {
      if (type.equals(JoinType.RIGHTOUTER)
          || type.equals(JoinType.FULLOUTER)) {
        if (conf.getBoolVar(HiveConf.ConfVars.HIVEOUTERJOINSUPPORTSFILTERS)) {
          joinTree.getFilters().get(1).add(joinCond);
        } else {
          LOG.warn(ErrorMsg.OUTERJOIN_USES_FILTERS);
          joinTree.getFiltersForPushing().get(1).add(joinCond);
        }
      } else {
        joinTree.getFiltersForPushing().get(1).add(joinCond);
      }
    }

  }

  @SuppressWarnings("rawtypes")
  private void parseJoinCondition(QBJoinTree joinTree, ASTNode joinCond, List<String> leftSrc,
      Map<String, Operator> aliasToOpInfo)
      throws SemanticException {
    if (joinCond == null) {
      return;
    }
    JoinCond cond = joinTree.getJoinCond()[0];

    JoinType type = cond.getJoinType();
    parseJoinCondition(joinTree, joinCond, leftSrc, type, aliasToOpInfo);

    List<ArrayList<ASTNode>> filters = joinTree.getFilters();
    if (type == JoinType.LEFTOUTER || type == JoinType.FULLOUTER) {
      joinTree.addFilterMapping(cond.getLeft(), cond.getRight(), filters.get(0).size());
    }
    if (type == JoinType.RIGHTOUTER || type == JoinType.FULLOUTER) {
      joinTree.addFilterMapping(cond.getRight(), cond.getLeft(), filters.get(1).size());
    }
  }

  /**
   * Parse the join condition. If the condition is a join condition, throw an
   * error if it is not an equality. Otherwise, break it into left and right
   * expressions and store in the join tree. If the condition is a join filter,
   * add it to the filter list of join tree. The join condition can contains
   * conditions on both the left and tree trees and filters on either.
   * Currently, we only support equi-joins, so we throw an error if the
   * condition involves both subtrees and is not a equality. Also, we only
   * support AND i.e ORs are not supported currently as their semantics are not
   * very clear, may lead to data explosion and there is no usecase.
   *
   * @param joinTree
   *          jointree to be populated
   * @param joinCond
   *          join condition
   * @param leftSrc
   *          left sources
   * @throws SemanticException
   */
  @SuppressWarnings("rawtypes")
  private void parseJoinCondition(QBJoinTree joinTree, ASTNode joinCond,
      List<String> leftSrc, JoinType type,
      Map<String, Operator> aliasToOpInfo) throws SemanticException {
    if (joinCond == null) {
      return;
    }

    switch (joinCond.getToken().getType()) {
    case HiveParser.KW_OR:
      throw new SemanticException(ErrorMsg.INVALID_JOIN_CONDITION_3
          .getMsg(joinCond));

    case HiveParser.KW_AND:
      parseJoinCondition(joinTree, (ASTNode) joinCond.getChild(0), leftSrc, type, aliasToOpInfo);
      parseJoinCondition(joinTree, (ASTNode) joinCond.getChild(1), leftSrc, type, aliasToOpInfo);
      break;

    case HiveParser.EQUAL_NS:
    case HiveParser.EQUAL:
      ASTNode leftCondn = (ASTNode) joinCond.getChild(0);
      ArrayList<String> leftCondAl1 = new ArrayList<String>();
      ArrayList<String> leftCondAl2 = new ArrayList<String>();
      parseJoinCondPopulateAlias(joinTree, leftCondn, leftCondAl1, leftCondAl2,
          null, aliasToOpInfo);

      ASTNode rightCondn = (ASTNode) joinCond.getChild(1);
      ArrayList<String> rightCondAl1 = new ArrayList<String>();
      ArrayList<String> rightCondAl2 = new ArrayList<String>();
      parseJoinCondPopulateAlias(joinTree, rightCondn, rightCondAl1,
          rightCondAl2, null, aliasToOpInfo);

      // is it a filter or a join condition
      // if it is filter see if it can be pushed above the join
      // filter cannot be pushed if
      // * join is full outer or
      // * join is left outer and filter is on left alias or
      // * join is right outer and filter is on right alias
      if (((leftCondAl1.size() != 0) && (leftCondAl2.size() != 0))
          || ((rightCondAl1.size() != 0) && (rightCondAl2.size() != 0))) {
        throw new SemanticException(ErrorMsg.INVALID_JOIN_CONDITION_1
            .getMsg(joinCond));
      }

      applyEqualityPredicateToQBJoinTree(joinTree, type, leftSrc,
          joinCond, leftCondn, rightCondn,
          leftCondAl1, leftCondAl2,
          rightCondAl1, rightCondAl2);

      break;

    default:
      boolean isFunction = (joinCond.getType() == HiveParser.TOK_FUNCTION);

      // Create all children
      int childrenBegin = (isFunction ? 1 : 0);
      ArrayList<ArrayList<String>> leftAlias = new ArrayList<ArrayList<String>>(
          joinCond.getChildCount() - childrenBegin);
      ArrayList<ArrayList<String>> rightAlias = new ArrayList<ArrayList<String>>(
          joinCond.getChildCount() - childrenBegin);
      for (int ci = 0; ci < joinCond.getChildCount() - childrenBegin; ci++) {
        ArrayList<String> left = new ArrayList<String>();
        ArrayList<String> right = new ArrayList<String>();
        leftAlias.add(left);
        rightAlias.add(right);
      }

      for (int ci = childrenBegin; ci < joinCond.getChildCount(); ci++) {
        parseJoinCondPopulateAlias(joinTree, (ASTNode) joinCond.getChild(ci),
            leftAlias.get(ci - childrenBegin), rightAlias.get(ci
                - childrenBegin), null, aliasToOpInfo);
      }

      boolean leftAliasNull = true;
      for (ArrayList<String> left : leftAlias) {
        if (left.size() != 0) {
          leftAliasNull = false;
          break;
        }
      }

      boolean rightAliasNull = true;
      for (ArrayList<String> right : rightAlias) {
        if (right.size() != 0) {
          rightAliasNull = false;
          break;
        }
      }

      if (!leftAliasNull && !rightAliasNull) {
        throw new SemanticException(ErrorMsg.INVALID_JOIN_CONDITION_1
            .getMsg(joinCond));
      }

      if (!leftAliasNull) {
        if (type.equals(JoinType.LEFTOUTER)
            || type.equals(JoinType.FULLOUTER)) {
          if (conf.getBoolVar(HiveConf.ConfVars.HIVEOUTERJOINSUPPORTSFILTERS)) {
            joinTree.getFilters().get(0).add(joinCond);
          } else {
            LOG.warn(ErrorMsg.OUTERJOIN_USES_FILTERS);
            joinTree.getFiltersForPushing().get(0).add(joinCond);
          }
        } else {
          joinTree.getFiltersForPushing().get(0).add(joinCond);
        }
      } else {
        if (type.equals(JoinType.RIGHTOUTER)
            || type.equals(JoinType.FULLOUTER)) {
          if (conf.getBoolVar(HiveConf.ConfVars.HIVEOUTERJOINSUPPORTSFILTERS)) {
            joinTree.getFilters().get(1).add(joinCond);
          } else {
            LOG.warn(ErrorMsg.OUTERJOIN_USES_FILTERS);
            joinTree.getFiltersForPushing().get(1).add(joinCond);
          }
        } else {
          joinTree.getFiltersForPushing().get(1).add(joinCond);
        }
      }

      break;
    }
  }

  @SuppressWarnings("rawtypes")
  private void extractJoinCondsFromWhereClause(QBJoinTree joinTree, QB qb, String dest, ASTNode predicate,
      Map<String, Operator> aliasToOpInfo) throws SemanticException {

    switch (predicate.getType()) {
    case HiveParser.KW_AND:
      extractJoinCondsFromWhereClause(joinTree, qb, dest,
          (ASTNode) predicate.getChild(0), aliasToOpInfo);
      extractJoinCondsFromWhereClause(joinTree, qb, dest,
          (ASTNode) predicate.getChild(1), aliasToOpInfo);
      break;
    case HiveParser.EQUAL_NS:
    case HiveParser.EQUAL:

      ASTNode leftCondn = (ASTNode) predicate.getChild(0);
      ArrayList<String> leftCondAl1 = new ArrayList<String>();
      ArrayList<String> leftCondAl2 = new ArrayList<String>();
      try {
        parseJoinCondPopulateAlias(joinTree, leftCondn, leftCondAl1, leftCondAl2,
          null, aliasToOpInfo);
      } catch(SemanticException se) {
        // suppress here; if it is a real issue will get caught in where clause handling.
        return;
      }

      ASTNode rightCondn = (ASTNode) predicate.getChild(1);
      ArrayList<String> rightCondAl1 = new ArrayList<String>();
      ArrayList<String> rightCondAl2 = new ArrayList<String>();
      try {
        parseJoinCondPopulateAlias(joinTree, rightCondn, rightCondAl1,
            rightCondAl2, null, aliasToOpInfo);
      } catch(SemanticException se) {
        // suppress here; if it is a real issue will get caught in where clause handling.
        return;
      }

      if (((leftCondAl1.size() != 0) && (leftCondAl2.size() != 0))
          || ((rightCondAl1.size() != 0) && (rightCondAl2.size() != 0))) {
        // this is not a join condition.
        return;
      }

      if (((leftCondAl1.size() == 0) && (leftCondAl2.size() == 0))
          || ((rightCondAl1.size() == 0) && (rightCondAl2.size() == 0))) {
        // this is not a join condition. Will get handled by predicate pushdown.
        return;
      }

      List<String> leftSrc = new ArrayList<String>();
      JoinCond cond = joinTree.getJoinCond()[0];
      JoinType type = cond.getJoinType();
      applyEqualityPredicateToQBJoinTree(joinTree, type, leftSrc,
          predicate, leftCondn, rightCondn,
          leftCondAl1, leftCondAl2,
          rightCondAl1, rightCondAl2);
      if (leftSrc.size() == 1) {
        joinTree.setLeftAlias(leftSrc.get(0));
      }

      // todo: hold onto this predicate, so that we don't add it to the Filter Operator.

      break;
    default:
      return;
    }
  }

  @SuppressWarnings("nls")
  public <T extends OperatorDesc> Operator<T> putOpInsertMap(Operator<T> op,
      RowResolver rr) {
    OpParseContext ctx = new OpParseContext(rr);
    opParseCtx.put(op, ctx);
    op.augmentPlan();
    return op;
  }

  @SuppressWarnings("nls")
  private Operator genHavingPlan(String dest, QB qb, Operator input,
      Map<String, Operator> aliasToOpInfo)
      throws SemanticException {

    ASTNode havingExpr = qb.getParseInfo().getHavingForClause(dest);

    OpParseContext inputCtx = opParseCtx.get(input);
    RowResolver inputRR = inputCtx.getRowResolver();
    Map<ASTNode, String> exprToColumnAlias = qb.getParseInfo().getAllExprToColumnAlias();
    for (ASTNode astNode : exprToColumnAlias.keySet()) {
      if (inputRR.getExpression(astNode) != null) {
        inputRR.put("", exprToColumnAlias.get(astNode), inputRR.getExpression(astNode));
      }
    }
    ASTNode condn = (ASTNode) havingExpr.getChild(0);

    /*
     * Now a having clause can contain a SubQuery predicate;
     * so we invoke genFilterPlan to handle SubQuery algebraic transformation,
     * just as is done for SubQuery predicates appearing in the Where Clause.
     */
    Operator output = genFilterPlan(condn, qb, input, aliasToOpInfo, true);
    output = putOpInsertMap(output, inputRR);
    return output;
  }

  private Operator genPlanForSubQueryPredicate(
      QB qbSQ,
      ISubQueryJoinInfo subQueryPredicate) throws SemanticException {
    qbSQ.setSubQueryDef(subQueryPredicate.getSubQuery());
    Phase1Ctx ctx_1 = initPhase1Ctx();
    doPhase1(subQueryPredicate.getSubQueryAST(), qbSQ, ctx_1, null);
    getMetaData(qbSQ);
    Operator op = genPlan(qbSQ);
    return op;
  }

  @SuppressWarnings("nls")
  private Operator genFilterPlan(ASTNode searchCond, QB qb, Operator input,
      Map<String, Operator> aliasToOpInfo,
      boolean forHavingClause)
      throws SemanticException {

    OpParseContext inputCtx = opParseCtx.get(input);
    RowResolver inputRR = inputCtx.getRowResolver();

    /*
     * Handling of SubQuery Expressions:
     * if "Where clause contains no SubQuery expressions" then
     *   -->[true] ===CONTINUE_FILTER_PROCESSING===
     * else
     *   -->[false] "extract SubQuery expressions\n from Where clause"
     *   if "this is a nested SubQuery or \nthere are more than 1 SubQuery expressions" then
     *     -->[yes] "throw Unsupported Error"
     *   else
     *     --> "Rewrite Search condition to \nremove SubQuery predicate"
     *      --> "build QBSubQuery"
     *        --> "extract correlated predicates \nfrom Where Clause"
     *        --> "add correlated Items to \nSelect List and Group By"
     *        --> "construct Join Predicate \nfrom correlation predicates"
     *     --> "Generate Plan for\n modified SubQuery"
     *     --> "Build the Join Condition\n for Parent Query to SubQuery join"
     *     --> "Build the QBJoinTree from the Join condition"
     *     --> "Update Parent Query Filter\n with any Post Join conditions"
     *     --> ===CONTINUE_FILTER_PROCESSING===
     *   endif
     * endif
     *
     * Support for Sub Queries in Having Clause:
     * - By and large this works the same way as SubQueries in the Where Clause.
     * - The one addum is the handling of aggregation expressions from the Outer Query
     *   appearing in correlation clauses.
     *   - So such correlating predicates are allowed:
     *        min(OuterQuert.x) = SubQuery.y
     *   - this requires special handling when converting to joins. See QBSubQuery.rewrite
     *     method method for detailed comments.
     */
    List<ASTNode> subQueriesInOriginalTree = SubQueryUtils.findSubQueries(searchCond);

    if ( subQueriesInOriginalTree.size() > 0 ) {

      /*
       * Restriction.9.m :: disallow nested SubQuery expressions.
       */
      if (qb.getSubQueryPredicateDef() != null  ) {
        throw new SemanticException(ErrorMsg.UNSUPPORTED_SUBQUERY_EXPRESSION.getMsg(
            subQueriesInOriginalTree.get(0), "Nested SubQuery expressions are not supported."));
      }

      /*
       * Restriction.8.m :: We allow only 1 SubQuery expression per Query.
       */
      if (subQueriesInOriginalTree.size() > 1 ) {

        throw new SemanticException(ErrorMsg.UNSUPPORTED_SUBQUERY_EXPRESSION.getMsg(
            subQueriesInOriginalTree.get(1), "Only 1 SubQuery expression is supported."));
      }

      /*
       * Clone the Search AST; apply all rewrites on the clone.
       */
      ASTNode clonedSearchCond = (ASTNode) SubQueryUtils.adaptor.dupTree(searchCond);
      List<ASTNode> subQueries = SubQueryUtils.findSubQueries(clonedSearchCond);

      for(int i=0; i < subQueries.size(); i++) {
        ASTNode subQueryAST = subQueries.get(i);
        ASTNode originalSubQueryAST = subQueriesInOriginalTree.get(i);

        int sqIdx = qb.incrNumSubQueryPredicates();
        clonedSearchCond = SubQueryUtils.rewriteParentQueryWhere(clonedSearchCond, subQueryAST);

        QBSubQuery subQuery = SubQueryUtils.buildSubQuery(qb.getId(),
            sqIdx, subQueryAST, originalSubQueryAST, ctx);

        if ( !forHavingClause ) {
          qb.setWhereClauseSubQueryPredicate(subQuery);
        } else {
          qb.setHavingClauseSubQueryPredicate(subQuery);
        }
        String havingInputAlias = null;

        if ( forHavingClause ) {
          havingInputAlias = "gby_sq" + sqIdx;
          aliasToOpInfo.put(havingInputAlias, input);
        }

        subQuery.validateAndRewriteAST(inputRR, forHavingClause, havingInputAlias, aliasToOpInfo.keySet());

        QB qbSQ = new QB(subQuery.getOuterQueryId(), subQuery.getAlias(), true);
        Operator sqPlanTopOp = genPlanForSubQueryPredicate(qbSQ, subQuery);
        aliasToOpInfo.put(subQuery.getAlias(), sqPlanTopOp);
        RowResolver sqRR = opParseCtx.get(sqPlanTopOp).getRowResolver();

        /*
         * Check.5.h :: For In and Not In the SubQuery must implicitly or
         * explicitly only contain one select item.
         */
        if ( subQuery.getOperator().getType() != SubQueryType.EXISTS &&
            subQuery.getOperator().getType() != SubQueryType.NOT_EXISTS &&
            sqRR.getColumnInfos().size() -
               subQuery.getNumOfCorrelationExprsAddedToSQSelect() > 1 ) {
          throw new SemanticException(ErrorMsg.INVALID_SUBQUERY_EXPRESSION.getMsg(
              subQueryAST, "SubQuery can contain only 1 item in Select List."));
        }

        /*
         * If this is a Not In SubQuery Predicate then Join in the Null Check SubQuery.
         * See QBSubQuery.NotInCheck for details on why and how this is constructed.
         */
        if ( subQuery.getNotInCheck() != null ) {
          QBSubQuery.NotInCheck notInCheck = subQuery.getNotInCheck();
          notInCheck.setSQRR(sqRR);
          QB qbSQ_nic = new QB(subQuery.getOuterQueryId(), notInCheck.getAlias(), true);
          Operator sqnicPlanTopOp = genPlanForSubQueryPredicate(qbSQ_nic, notInCheck);
          aliasToOpInfo.put(notInCheck.getAlias(), sqnicPlanTopOp);
          QBJoinTree joinTree_nic = genSQJoinTree(qb, notInCheck,
              input,
              aliasToOpInfo);
          pushJoinFilters(qb, joinTree_nic, aliasToOpInfo, false);
          input = genJoinOperator(qbSQ_nic, joinTree_nic, aliasToOpInfo, input);
          inputRR = opParseCtx.get(input).getRowResolver();
          if ( forHavingClause ) {
            aliasToOpInfo.put(havingInputAlias, input);
          }
        }

        /*
         * Gen Join between outer Operator and SQ op
         */
        subQuery.buildJoinCondition(inputRR, sqRR, forHavingClause, havingInputAlias);
        QBJoinTree joinTree = genSQJoinTree(qb, subQuery,
            input,
            aliasToOpInfo);
        /*
         * push filters only for this QBJoinTree. Child QBJoinTrees have already been handled.
         */
        pushJoinFilters(qb, joinTree, aliasToOpInfo, false);
        input = genJoinOperator(qbSQ, joinTree, aliasToOpInfo, input);
        searchCond = subQuery.updateOuterQueryFilter(clonedSearchCond);
      }
    }

    return genFilterPlan(qb, searchCond, input);
  }

  /**
   * create a filter plan. The condition and the inputs are specified.
   *
   * @param qb
   *          current query block
   * @param condn
   *          The condition to be resolved
   * @param input
   *          the input operator
   */
  @SuppressWarnings("nls")
  private Operator genFilterPlan(QB qb, ASTNode condn, Operator input)
      throws SemanticException {

    OpParseContext inputCtx = opParseCtx.get(input);
    RowResolver inputRR = inputCtx.getRowResolver();
    Operator output = putOpInsertMap(OperatorFactory.getAndMakeChild(
        new FilterDesc(genExprNodeDesc(condn, inputRR), false), new RowSchema(
            inputRR.getColumnInfos()), input), inputRR);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Created Filter Plan for " + qb.getId() + " row schema: "
          + inputRR.toString());
    }
    return output;
  }

  /*
   * for inner joins push a 'is not null predicate' to the join sources for
   * every non nullSafe predicate.
   */
  private Operator genNotNullFilterForJoinSourcePlan(QB qb, Operator input,
      QBJoinTree joinTree, ExprNodeDesc[] joinKeys) throws SemanticException {

    if (qb == null || joinTree == null) {
      return input;
    }

    if (!joinTree.getNoOuterJoin()) {
      return input;
    }

    if (joinKeys == null || joinKeys.length == 0) {
      return input;
    }
    Map<Integer, ExprNodeDesc> hashes = new HashMap<Integer, ExprNodeDesc>();
    if (input instanceof FilterOperator) {
      ExprNodeDescUtils.getExprNodeColumnDesc(Arrays.asList(((FilterDesc)input.getConf()).getPredicate()), hashes);
    }
    ExprNodeDesc filterPred = null;
    List<Boolean> nullSafes = joinTree.getNullSafes();
    for (int i = 0; i < joinKeys.length; i++) {
      if (nullSafes.get(i) || (joinKeys[i] instanceof ExprNodeColumnDesc &&
         ((ExprNodeColumnDesc)joinKeys[i]).getIsPartitionColOrVirtualCol())) {
        // no need to generate is not null predicate for partitioning or
        // virtual column, since those columns can never be null.
        continue;
      }
      if(null != hashes.get(joinKeys[i].hashCode())) {
        // there is already a predicate on this src.
        continue;
      }
      List<ExprNodeDesc> args = new ArrayList<ExprNodeDesc>();
      args.add(joinKeys[i]);
      ExprNodeDesc nextExpr = ExprNodeGenericFuncDesc.newInstance(
          FunctionRegistry.getFunctionInfo("isnotnull").getGenericUDF(), args);
      filterPred = filterPred == null ? nextExpr : ExprNodeDescUtils
          .mergePredicates(filterPred, nextExpr);
    }

    if (filterPred == null) {
      return input;
    }

    OpParseContext inputCtx = opParseCtx.get(input);
    RowResolver inputRR = inputCtx.getRowResolver();

    if (input instanceof FilterOperator) {
      FilterOperator f = (FilterOperator) input;
      List<ExprNodeDesc> preds = new ArrayList<ExprNodeDesc>();
      preds.add(f.getConf().getPredicate());
      preds.add(filterPred);
      f.getConf().setPredicate(ExprNodeDescUtils.mergePredicates(preds));

      return input;
    }

    Operator output = putOpInsertMap(OperatorFactory.getAndMakeChild(
        new FilterDesc(filterPred, false),
        new RowSchema(inputRR.getColumnInfos()), input), inputRR);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Created Filter Plan for " + qb.getId() + " row schema: "
          + inputRR.toString());
    }
    return output;
  }

  @SuppressWarnings("nls")
  // TODO: make aliases unique, otherwise needless rewriting takes place
  Integer genColListRegex(String colRegex, String tabAlias, ASTNode sel,
    ArrayList<ExprNodeDesc> col_list, HashSet<ColumnInfo> excludeCols, RowResolver input,
    RowResolver colSrcRR, Integer pos, RowResolver output, List<String> aliases,
    boolean ensureUniqueCols) throws SemanticException {

    if (colSrcRR == null) {
      colSrcRR = input;
    }
    // The table alias should exist
    if (tabAlias != null && !colSrcRR.hasTableAlias(tabAlias)) {
      throw new SemanticException(ErrorMsg.INVALID_TABLE_ALIAS.getMsg(sel));
    }

    // TODO: Have to put in the support for AS clause
    Pattern regex = null;
    try {
      regex = Pattern.compile(colRegex, Pattern.CASE_INSENSITIVE);
    } catch (PatternSyntaxException e) {
      throw new SemanticException(ErrorMsg.INVALID_COLUMN.getMsg(sel, e
          .getMessage()));
    }

    StringBuilder replacementText = new StringBuilder();
    int matched = 0;
    // add empty string to the list of aliases. Some operators (ex. GroupBy) add
    // ColumnInfos for table alias "".
    if (!aliases.contains("")) {
      aliases.add("");
    }
    /*
     * track the input ColumnInfos that are added to the output.
     * if a columnInfo has multiple mappings; then add the column only once,
     * but carry the mappings forward.
     */
    Map<ColumnInfo, ColumnInfo> inputColsProcessed = new HashMap<ColumnInfo, ColumnInfo>();
    // For expr "*", aliases should be iterated in the order they are specified
    // in the query.
    for (String alias : aliases) {
      HashMap<String, ColumnInfo> fMap = colSrcRR.getFieldMap(alias);
      if (fMap == null) {
        continue;
      }
      // For the tab.* case, add all the columns to the fieldList
      // from the input schema
      for (Map.Entry<String, ColumnInfo> entry : fMap.entrySet()) {
        ColumnInfo colInfo = entry.getValue();
        if (excludeCols != null && excludeCols.contains(colInfo)) {
          continue; // This was added during plan generation.
        }
        // First, look up the column from the source against which * is to be resolved.
        // We'd later translated this into the column from proper input, if it's valid.
        // TODO: excludeCols may be possible to remove using the same technique.
        String name = colInfo.getInternalName();
        String[] tmp = colSrcRR.reverseLookup(name);

        // Skip the colinfos which are not for this particular alias
        if (tabAlias != null && !tmp[0].equalsIgnoreCase(tabAlias)) {
          continue;
        }

        if (colInfo.getIsVirtualCol() && colInfo.isHiddenVirtualCol()) {
          continue;
        }

        // Not matching the regex?
        if (!regex.matcher(tmp[1]).matches()) {
          continue;
        }

        // If input (GBY) is different than the source of columns, find the same column in input.
        // TODO: This is fraught with peril.
        if (input != colSrcRR) {
          colInfo = input.get(tabAlias, tmp[1]);
          if (colInfo == null) {
            LOG.error("Cannot find colInfo for " + tabAlias + "." + tmp[1]
                + ", derived from [" + colSrcRR + "], in [" + input + "]");
            throw new SemanticException(ErrorMsg.NON_KEY_EXPR_IN_GROUPBY, tmp[1]);
          }
          String oldCol = null;
          if (LOG.isDebugEnabled()) {
            oldCol = name + " => " + (tmp == null ? "null" : (tmp[0] + "." + tmp[1]));
          }
          name = colInfo.getInternalName();
          tmp = input.reverseLookup(name);
          if (LOG.isDebugEnabled()) {
            String newCol = name + " => " + (tmp == null ? "null" : (tmp[0] + "." + tmp[1]));
            LOG.debug("Translated [" + oldCol + "] to [" + newCol + "]");
          }
        }

        ColumnInfo oColInfo = inputColsProcessed.get(colInfo);
        if (oColInfo == null) {
          ExprNodeColumnDesc expr = new ExprNodeColumnDesc(colInfo.getType(),
              name, colInfo.getTabAlias(), colInfo.getIsVirtualCol(),
              colInfo.isSkewedCol());
          col_list.add(expr);
          oColInfo = new ColumnInfo(getColumnInternalName(pos),
              colInfo.getType(), colInfo.getTabAlias(),
              colInfo.getIsVirtualCol(), colInfo.isHiddenVirtualCol());
          inputColsProcessed.put(colInfo, oColInfo);
        }
        if (ensureUniqueCols) {
          if (!output.putWithCheck(tmp[0], tmp[1], null, oColInfo)) {
            throw new CalciteSemanticException("Cannot add column to RR: " + tmp[0] + "." + tmp[1]
                + " => " + oColInfo + " due to duplication, see previous warnings");
          }
        } else {
          output.put(tmp[0], tmp[1], oColInfo);
        }
        pos = Integer.valueOf(pos.intValue() + 1);
        matched++;

        if (unparseTranslator.isEnabled()) {
          if (replacementText.length() > 0) {
            replacementText.append(", ");
          }
          replacementText.append(HiveUtils.unparseIdentifier(tmp[0], conf));
          replacementText.append(".");
          replacementText.append(HiveUtils.unparseIdentifier(tmp[1], conf));
        }
      }
    }
    if (matched == 0) {
      throw new SemanticException(ErrorMsg.INVALID_COLUMN.getMsg(sel));
    }

    if (unparseTranslator.isEnabled()) {
      unparseTranslator.addTranslation(sel, replacementText.toString());
    }
    return pos;
  }

  public static String getColumnInternalName(int pos) {
    return HiveConf.getColumnInternalName(pos);
  }

  private String getScriptProgName(String cmd) {
    int end = cmd.indexOf(" ");
    return (end == -1) ? cmd : cmd.substring(0, end);
  }

  private String getScriptArgs(String cmd) {
    int end = cmd.indexOf(" ");
    return (end == -1) ? "" : cmd.substring(end, cmd.length());
  }

  static int getPositionFromInternalName(String internalName) {
    return HiveConf.getPositionFromInternalName(internalName);
  }

  private String fetchFilesNotInLocalFilesystem(String cmd) {
    SessionState ss = SessionState.get();
    String progName = getScriptProgName(cmd);

    if (SessionState.canDownloadResource(progName)) {
      String filePath = ss.add_resource(ResourceType.FILE, progName, true);
      Path p = new Path(filePath);
      String fileName = p.getName();
      String scriptArgs = getScriptArgs(cmd);
      String finalCmd = fileName + scriptArgs;
      return finalCmd;
    }

    return cmd;
  }

  private TableDesc getTableDescFromSerDe(ASTNode child, String cols,
      String colTypes, boolean defaultCols) throws SemanticException {
    if (child.getType() == HiveParser.TOK_SERDENAME) {
      String serdeName = unescapeSQLString(child.getChild(0).getText());
      Class<? extends Deserializer> serdeClass = null;

      try {
        serdeClass = (Class<? extends Deserializer>) Class.forName(serdeName,
            true, Utilities.getSessionSpecifiedClassLoader());
      } catch (ClassNotFoundException e) {
        throw new SemanticException(e);
      }

      TableDesc tblDesc = PlanUtils.getTableDesc(serdeClass, Integer
          .toString(Utilities.tabCode), cols, colTypes, defaultCols);
      // copy all the properties
      if (child.getChildCount() == 2) {
        ASTNode prop = (ASTNode) ((ASTNode) child.getChild(1)).getChild(0);
        for (int propChild = 0; propChild < prop.getChildCount(); propChild++) {
          String key = unescapeSQLString(prop.getChild(propChild).getChild(0)
              .getText());
          String value = unescapeSQLString(prop.getChild(propChild).getChild(1)
              .getText());
          tblDesc.getProperties().setProperty(key, value);
        }
      }
      return tblDesc;
    } else if (child.getType() == HiveParser.TOK_SERDEPROPS) {
      TableDesc tblDesc = PlanUtils.getDefaultTableDesc(Integer
          .toString(Utilities.ctrlaCode), cols, colTypes, defaultCols);
      int numChildRowFormat = child.getChildCount();
      for (int numC = 0; numC < numChildRowFormat; numC++) {
        ASTNode rowChild = (ASTNode) child.getChild(numC);
        switch (rowChild.getToken().getType()) {
        case HiveParser.TOK_TABLEROWFORMATFIELD:
          String fieldDelim = unescapeSQLString(rowChild.getChild(0).getText());
          tblDesc.getProperties()
              .setProperty(serdeConstants.FIELD_DELIM, fieldDelim);
          tblDesc.getProperties().setProperty(serdeConstants.SERIALIZATION_FORMAT,
              fieldDelim);

          if (rowChild.getChildCount() >= 2) {
            String fieldEscape = unescapeSQLString(rowChild.getChild(1)
                .getText());
            tblDesc.getProperties().setProperty(serdeConstants.ESCAPE_CHAR,
                fieldEscape);
          }
          break;
        case HiveParser.TOK_TABLEROWFORMATCOLLITEMS:
          tblDesc.getProperties().setProperty(serdeConstants.COLLECTION_DELIM,
              unescapeSQLString(rowChild.getChild(0).getText()));
          break;
        case HiveParser.TOK_TABLEROWFORMATMAPKEYS:
          tblDesc.getProperties().setProperty(serdeConstants.MAPKEY_DELIM,
              unescapeSQLString(rowChild.getChild(0).getText()));
          break;
        case HiveParser.TOK_TABLEROWFORMATLINES:
          String lineDelim = unescapeSQLString(rowChild.getChild(0).getText());
          tblDesc.getProperties().setProperty(serdeConstants.LINE_DELIM, lineDelim);
          if (!lineDelim.equals("\n") && !lineDelim.equals("10")) {
            throw new SemanticException(generateErrorMessage(rowChild,
                ErrorMsg.LINES_TERMINATED_BY_NON_NEWLINE.getMsg()));
          }
          break;
        case HiveParser.TOK_TABLEROWFORMATNULL:
          String nullFormat = unescapeSQLString(rowChild.getChild(0).getText());
          tblDesc.getProperties().setProperty(serdeConstants.SERIALIZATION_NULL_FORMAT,
              nullFormat);
          break;
        default:
          assert false;
        }
      }

      return tblDesc;
    }

    // should never come here
    return null;
  }

  private void failIfColAliasExists(Set<String> nameSet, String name)
      throws SemanticException {
    if (nameSet.contains(name)) {
      throw new SemanticException(ErrorMsg.COLUMN_ALIAS_ALREADY_EXISTS
          .getMsg(name));
    }
    nameSet.add(name);
  }

  @SuppressWarnings("nls")
  private Operator genScriptPlan(ASTNode trfm, QB qb, Operator input)
      throws SemanticException {
    // If there is no "AS" clause, the output schema will be "key,value"
    ArrayList<ColumnInfo> outputCols = new ArrayList<ColumnInfo>();
    int inputSerDeNum = 1, inputRecordWriterNum = 2;
    int outputSerDeNum = 4, outputRecordReaderNum = 5;
    int outputColsNum = 6;
    boolean outputColNames = false, outputColSchemas = false;
    int execPos = 3;
    boolean defaultOutputCols = false;

    // Go over all the children
    if (trfm.getChildCount() > outputColsNum) {
      ASTNode outCols = (ASTNode) trfm.getChild(outputColsNum);
      if (outCols.getType() == HiveParser.TOK_ALIASLIST) {
        outputColNames = true;
      } else if (outCols.getType() == HiveParser.TOK_TABCOLLIST) {
        outputColSchemas = true;
      }
    }

    // If column type is not specified, use a string
    if (!outputColNames && !outputColSchemas) {
      String intName = getColumnInternalName(0);
      ColumnInfo colInfo = new ColumnInfo(intName,
          TypeInfoFactory.stringTypeInfo, null, false);
      colInfo.setAlias("key");
      outputCols.add(colInfo);
      intName = getColumnInternalName(1);
      colInfo = new ColumnInfo(intName, TypeInfoFactory.stringTypeInfo, null,
          false);
      colInfo.setAlias("value");
      outputCols.add(colInfo);
      defaultOutputCols = true;
    } else {
      ASTNode collist = (ASTNode) trfm.getChild(outputColsNum);
      int ccount = collist.getChildCount();

      Set<String> colAliasNamesDuplicateCheck = new HashSet<String>();
      if (outputColNames) {
        for (int i = 0; i < ccount; ++i) {
          String colAlias = unescapeIdentifier(((ASTNode) collist.getChild(i))
              .getText());
          failIfColAliasExists(colAliasNamesDuplicateCheck, colAlias);
          String intName = getColumnInternalName(i);
          ColumnInfo colInfo = new ColumnInfo(intName,
              TypeInfoFactory.stringTypeInfo, null, false);
          colInfo.setAlias(colAlias);
          outputCols.add(colInfo);
        }
      } else {
        for (int i = 0; i < ccount; ++i) {
          ASTNode child = (ASTNode) collist.getChild(i);
          assert child.getType() == HiveParser.TOK_TABCOL;
          String colAlias = unescapeIdentifier(((ASTNode) child.getChild(0))
              .getText());
          failIfColAliasExists(colAliasNamesDuplicateCheck, colAlias);
          String intName = getColumnInternalName(i);
          ColumnInfo colInfo = new ColumnInfo(intName, TypeInfoUtils
              .getTypeInfoFromTypeString(getTypeStringFromAST((ASTNode) child
                  .getChild(1))), null, false);
          colInfo.setAlias(colAlias);
          outputCols.add(colInfo);
        }
      }
    }

    RowResolver out_rwsch = new RowResolver();
    StringBuilder columns = new StringBuilder();
    StringBuilder columnTypes = new StringBuilder();

    for (int i = 0; i < outputCols.size(); ++i) {
      if (i != 0) {
        columns.append(",");
        columnTypes.append(",");
      }

      columns.append(outputCols.get(i).getInternalName());
      columnTypes.append(outputCols.get(i).getType().getTypeName());

      out_rwsch.put(qb.getParseInfo().getAlias(), outputCols.get(i).getAlias(),
          outputCols.get(i));
    }

    StringBuilder inpColumns = new StringBuilder();
    StringBuilder inpColumnTypes = new StringBuilder();
    ArrayList<ColumnInfo> inputSchema = opParseCtx.get(input).getRowResolver()
        .getColumnInfos();
    for (int i = 0; i < inputSchema.size(); ++i) {
      if (i != 0) {
        inpColumns.append(",");
        inpColumnTypes.append(",");
      }

      inpColumns.append(inputSchema.get(i).getInternalName());
      inpColumnTypes.append(inputSchema.get(i).getType().getTypeName());
    }

    TableDesc outInfo;
    TableDesc errInfo;
    TableDesc inInfo;
    String defaultSerdeName = conf.getVar(HiveConf.ConfVars.HIVESCRIPTSERDE);
    Class<? extends Deserializer> serde;

    try {
      serde = (Class<? extends Deserializer>) Class.forName(defaultSerdeName,
          true, Utilities.getSessionSpecifiedClassLoader());
    } catch (ClassNotFoundException e) {
      throw new SemanticException(e);
    }

    int fieldSeparator = Utilities.tabCode;
    if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVESCRIPTESCAPE)) {
      fieldSeparator = Utilities.ctrlaCode;
    }

    // Input and Output Serdes
    if (trfm.getChild(inputSerDeNum).getChildCount() > 0) {
      inInfo = getTableDescFromSerDe((ASTNode) (((ASTNode) trfm
          .getChild(inputSerDeNum))).getChild(0), inpColumns.toString(),
          inpColumnTypes.toString(), false);
    } else {
      inInfo = PlanUtils.getTableDesc(serde, Integer
          .toString(fieldSeparator), inpColumns.toString(), inpColumnTypes
          .toString(), false, true);
    }

    if (trfm.getChild(outputSerDeNum).getChildCount() > 0) {
      outInfo = getTableDescFromSerDe((ASTNode) (((ASTNode) trfm
          .getChild(outputSerDeNum))).getChild(0), columns.toString(),
          columnTypes.toString(), false);
      // This is for backward compatibility. If the user did not specify the
      // output column list, we assume that there are 2 columns: key and value.
      // However, if the script outputs: col1, col2, col3 seperated by TAB, the
      // requirement is: key is col and value is (col2 TAB col3)
    } else {
      outInfo = PlanUtils.getTableDesc(serde, Integer
          .toString(fieldSeparator), columns.toString(), columnTypes
          .toString(), defaultOutputCols);
    }

    // Error stream always uses the default serde with a single column
    errInfo = PlanUtils.getTableDesc(serde, Integer.toString(Utilities.tabCode), "KEY");

    // Output record readers
    Class<? extends RecordReader> outRecordReader = getRecordReader((ASTNode) trfm
        .getChild(outputRecordReaderNum));
    Class<? extends RecordWriter> inRecordWriter = getRecordWriter((ASTNode) trfm
        .getChild(inputRecordWriterNum));
    Class<? extends RecordReader> errRecordReader = getDefaultRecordReader();

    Operator output = putOpInsertMap(OperatorFactory.getAndMakeChild(
        new ScriptDesc(
            fetchFilesNotInLocalFilesystem(stripQuotes(trfm.getChild(execPos).getText())),
            inInfo, inRecordWriter, outInfo, outRecordReader, errRecordReader, errInfo),
        new RowSchema(out_rwsch.getColumnInfos()), input), out_rwsch);
    output.setColumnExprMap(new HashMap<String, ExprNodeDesc>());  // disable backtracking

    // Add URI entity for transform script. script assumed t be local unless downloadable
    if (conf.getBoolVar(ConfVars.HIVE_CAPTURE_TRANSFORM_ENTITY)) {
      String scriptCmd = getScriptProgName(stripQuotes(trfm.getChild(execPos).getText()));
      getInputs().add(new ReadEntity(new Path(scriptCmd),
          !SessionState.canDownloadResource(scriptCmd)));
    }

    return output;
  }

  private Class<? extends RecordReader> getRecordReader(ASTNode node)
      throws SemanticException {
    String name;

    if (node.getChildCount() == 0) {
      name = conf.getVar(HiveConf.ConfVars.HIVESCRIPTRECORDREADER);
    } else {
      name = unescapeSQLString(node.getChild(0).getText());
    }

    try {
      return (Class<? extends RecordReader>) Class.forName(name, true,
          Utilities.getSessionSpecifiedClassLoader());
    } catch (ClassNotFoundException e) {
      throw new SemanticException(e);
    }
  }

  private Class<? extends RecordReader> getDefaultRecordReader()
      throws SemanticException {
    String name;

    name = conf.getVar(HiveConf.ConfVars.HIVESCRIPTRECORDREADER);

    try {
      return (Class<? extends RecordReader>) Class.forName(name, true,
          Utilities.getSessionSpecifiedClassLoader());
    } catch (ClassNotFoundException e) {
      throw new SemanticException(e);
    }
  }

  private Class<? extends RecordWriter> getRecordWriter(ASTNode node)
      throws SemanticException {
    String name;

    if (node.getChildCount() == 0) {
      name = conf.getVar(HiveConf.ConfVars.HIVESCRIPTRECORDWRITER);
    } else {
      name = unescapeSQLString(node.getChild(0).getText());
    }

    try {
      return (Class<? extends RecordWriter>) Class.forName(name, true,
          Utilities.getSessionSpecifiedClassLoader());
    } catch (ClassNotFoundException e) {
      throw new SemanticException(e);
    }
  }

  protected List<Integer> getGroupingSetsForRollup(int size) {
    List<Integer> groupingSetKeys = new ArrayList<Integer>();
    for (int i = 0; i <= size; i++) {
      groupingSetKeys.add((1 << i) - 1);
    }
    return groupingSetKeys;
  }

  protected List<Integer> getGroupingSetsForCube(int size) {
    int count = 1 << size;
    List<Integer> results = new ArrayList<Integer>(count);
    for (int i = 0; i < count; ++i) {
      results.add(i);
    }
    return results;
  }

  // This function returns the grouping sets along with the grouping expressions
  // Even if rollups and cubes are present in the query, they are converted to
  // grouping sets at this point
  private ObjectPair<List<ASTNode>, List<Integer>> getGroupByGroupingSetsForClause(
      QBParseInfo parseInfo, String dest) throws SemanticException {
    List<Integer> groupingSets = new ArrayList<Integer>();
    List<ASTNode> groupByExprs = getGroupByForClause(parseInfo, dest);
    if (parseInfo.getDestRollups().contains(dest)) {
      groupingSets = getGroupingSetsForRollup(groupByExprs.size());
    } else if (parseInfo.getDestCubes().contains(dest)) {
      groupingSets = getGroupingSetsForCube(groupByExprs.size());
    } else if (parseInfo.getDestGroupingSets().contains(dest)) {
      groupingSets = getGroupingSets(groupByExprs, parseInfo, dest);
    }

    return new ObjectPair<List<ASTNode>, List<Integer>>(groupByExprs, groupingSets);
  }

  protected List<Integer> getGroupingSets(List<ASTNode> groupByExpr, QBParseInfo parseInfo,
      String dest) throws SemanticException {
    Map<String, Integer> exprPos = new HashMap<String, Integer>();
    for (int i = 0; i < groupByExpr.size(); ++i) {
      ASTNode node = groupByExpr.get(i);
      exprPos.put(node.toStringTree(), i);
    }

    ASTNode root = parseInfo.getGroupByForClause(dest);
    List<Integer> result = new ArrayList<Integer>(root == null ? 0 : root.getChildCount());
    if (root != null) {
      for (int i = 0; i < root.getChildCount(); ++i) {
        ASTNode child = (ASTNode) root.getChild(i);
        if (child.getType() != HiveParser.TOK_GROUPING_SETS_EXPRESSION) {
          continue;
        }
        int bitmap = 0;
        for (int j = 0; j < child.getChildCount(); ++j) {
          String treeAsString = child.getChild(j).toStringTree();
          Integer pos = exprPos.get(treeAsString);
          if (pos == null) {
            throw new SemanticException(
                generateErrorMessage((ASTNode) child.getChild(j),
                    ErrorMsg.HIVE_GROUPING_SETS_EXPR_NOT_IN_GROUPBY.getErrorCodedMsg()));
          }
          bitmap = setBit(bitmap, pos);
        }
        result.add(bitmap);
      }
    }
    if (checkForNoAggr(result)) {
      throw new SemanticException(
          ErrorMsg.HIVE_GROUPING_SETS_AGGR_NOFUNC.getMsg());
    }
    return result;
  }

  private boolean checkForNoAggr(List<Integer> bitmaps) {
    boolean ret = true;
    for (int mask : bitmaps) {
      ret &= mask == 0;
    }
    return ret;
  }

  private int setBit(int bitmap, int bitIdx) {
    return bitmap | (1 << bitIdx);
  }

  /**
   * This function is a wrapper of parseInfo.getGroupByForClause which
   * automatically translates SELECT DISTINCT a,b,c to SELECT a,b,c GROUP BY
   * a,b,c.
   */
  static List<ASTNode> getGroupByForClause(QBParseInfo parseInfo, String dest) {
    if (parseInfo.getSelForClause(dest).getToken().getType() == HiveParser.TOK_SELECTDI) {
      ASTNode selectExprs = parseInfo.getSelForClause(dest);
      List<ASTNode> result = new ArrayList<ASTNode>(selectExprs == null ? 0
          : selectExprs.getChildCount());
      if (selectExprs != null) {
        HashMap<String, ASTNode> windowingExprs = parseInfo.getWindowingExprsForClause(dest);

        for (int i = 0; i < selectExprs.getChildCount(); ++i) {
          if (((ASTNode) selectExprs.getChild(i)).getToken().getType() == HiveParser.TOK_HINTLIST) {
            continue;
          }
          // table.column AS alias
          ASTNode grpbyExpr = (ASTNode) selectExprs.getChild(i).getChild(0);
          /*
           * If this is handled by Windowing then ignore it.
           */
          if (windowingExprs != null && windowingExprs.containsKey(grpbyExpr.toStringTree())) {
            continue;
          }
          result.add(grpbyExpr);
        }
      }
      return result;
    } else {
      ASTNode grpByExprs = parseInfo.getGroupByForClause(dest);
      List<ASTNode> result = new ArrayList<ASTNode>(grpByExprs == null ? 0
          : grpByExprs.getChildCount());
      if (grpByExprs != null) {
        for (int i = 0; i < grpByExprs.getChildCount(); ++i) {
          ASTNode grpbyExpr = (ASTNode) grpByExprs.getChild(i);
          if (grpbyExpr.getType() != HiveParser.TOK_GROUPING_SETS_EXPRESSION) {
            result.add(grpbyExpr);
          }
        }
      }
      return result;
    }
  }

  static String[] getColAlias(ASTNode selExpr, String defaultName,
      RowResolver inputRR, boolean includeFuncName, int colNum) {
    String colAlias = null;
    String tabAlias = null;
    String[] colRef = new String[2];

    //for queries with a windowing expressions, the selexpr may have a third child
    if (selExpr.getChildCount() == 2 ||
        (selExpr.getChildCount() == 3 &&
        selExpr.getChild(2).getType() == HiveParser.TOK_WINDOWSPEC)) {
      // return zz for "xx + yy AS zz"
      colAlias = unescapeIdentifier(selExpr.getChild(1).getText());
      colRef[0] = tabAlias;
      colRef[1] = colAlias;
      return colRef;
    }

    ASTNode root = (ASTNode) selExpr.getChild(0);
    if (root.getType() == HiveParser.TOK_TABLE_OR_COL) {
      colAlias =
          BaseSemanticAnalyzer.unescapeIdentifier(root.getChild(0).getText());
      colRef[0] = tabAlias;
      colRef[1] = colAlias;
      return colRef;
    }

    if (root.getType() == HiveParser.DOT) {
      ASTNode tab = (ASTNode) root.getChild(0);
      if (tab.getType() == HiveParser.TOK_TABLE_OR_COL) {
        String t = unescapeIdentifier(tab.getChild(0).getText());
        if (inputRR.hasTableAlias(t)) {
          tabAlias = t;
        }
      }

      // Return zz for "xx.zz" and "xx.yy.zz"
      ASTNode col = (ASTNode) root.getChild(1);
      if (col.getType() == HiveParser.Identifier) {
        colAlias = unescapeIdentifier(col.getText());
      }
    }

    // if specified generate alias using func name
    if (includeFuncName && (root.getType() == HiveParser.TOK_FUNCTION)) {

      String expr_flattened = root.toStringTree();

      // remove all TOK tokens
      String expr_no_tok = expr_flattened.replaceAll("TOK_\\S+", "");

      // remove all non alphanumeric letters, replace whitespace spans with underscore
      String expr_formatted = expr_no_tok.replaceAll("\\W", " ").trim().replaceAll("\\s+", "_");

      // limit length to 20 chars
      if (expr_formatted.length() > AUTOGEN_COLALIAS_PRFX_MAXLENGTH) {
        expr_formatted = expr_formatted.substring(0, AUTOGEN_COLALIAS_PRFX_MAXLENGTH);
      }

      // append colnum to make it unique
      colAlias = expr_formatted.concat("_" + colNum);
    }

    if (colAlias == null) {
      // Return defaultName if selExpr is not a simple xx.yy.zz
      colAlias = defaultName + colNum;
    }

    colRef[0] = tabAlias;
    colRef[1] = colAlias;
    return colRef;
  }

  /**
   * Returns whether the pattern is a regex expression (instead of a normal
   * string). Normal string is a string with all alphabets/digits and "_".
   */
  static boolean isRegex(String pattern, HiveConf conf) {
    String qIdSupport = HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_QUOTEDID_SUPPORT);
    if ( "column".equals(qIdSupport)) {
      return false;
    }
    for (int i = 0; i < pattern.length(); i++) {
      if (!Character.isLetterOrDigit(pattern.charAt(i))
          && pattern.charAt(i) != '_') {
        return true;
      }
    }
    return false;
  }


  private Operator<?> genSelectPlan(String dest, QB qb, Operator<?> input,
      Operator<?> inputForSelectStar) throws SemanticException {
    ASTNode selExprList = qb.getParseInfo().getSelForClause(dest);
    Operator<?> op = genSelectPlan(selExprList, qb, input, inputForSelectStar, false);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Created Select Plan for clause: " + dest);
    }

    return op;
  }

  @SuppressWarnings("nls")
  private Operator<?> genSelectPlan(ASTNode selExprList, QB qb, Operator<?> input,
      Operator<?> inputForSelectStar, boolean outerLV) throws SemanticException {

    if (LOG.isDebugEnabled()) {
      LOG.debug("tree: " + selExprList.toStringTree());
    }

    ArrayList<ExprNodeDesc> col_list = new ArrayList<ExprNodeDesc>();
    RowResolver out_rwsch = new RowResolver();
    ASTNode trfm = null;
    Integer pos = Integer.valueOf(0);
    RowResolver inputRR = opParseCtx.get(input).getRowResolver();
    RowResolver starRR = null;
    if (inputForSelectStar != null && inputForSelectStar != input) {
      starRR = opParseCtx.get(inputForSelectStar).getRowResolver();
    }
    // SELECT * or SELECT TRANSFORM(*)
    boolean selectStar = false;
    int posn = 0;
    boolean hintPresent = (selExprList.getChild(0).getType() == HiveParser.TOK_HINTLIST);
    if (hintPresent) {
      posn++;
    }

    boolean isInTransform = (selExprList.getChild(posn).getChild(0).getType() ==
        HiveParser.TOK_TRANSFORM);
    if (isInTransform) {
      queryProperties.setUsesScript(true);
      globalLimitCtx.setHasTransformOrUDTF(true);
      trfm = (ASTNode) selExprList.getChild(posn).getChild(0);
    }

    // Detect queries of the form SELECT udtf(col) AS ...
    // by looking for a function as the first child, and then checking to see
    // if the function is a Generic UDTF. It's not as clean as TRANSFORM due to
    // the lack of a special token.
    boolean isUDTF = false;
    String udtfTableAlias = null;
    ArrayList<String> udtfColAliases = new ArrayList<String>();
    ASTNode udtfExpr = (ASTNode) selExprList.getChild(posn).getChild(0);
    GenericUDTF genericUDTF = null;

    int udtfExprType = udtfExpr.getType();
    if (udtfExprType == HiveParser.TOK_FUNCTION
        || udtfExprType == HiveParser.TOK_FUNCTIONSTAR) {
      String funcName = TypeCheckProcFactory.DefaultExprProcessor
          .getFunctionText(udtfExpr, true);
      FunctionInfo fi = FunctionRegistry.getFunctionInfo(funcName);
      if (fi != null) {
        genericUDTF = fi.getGenericUDTF();
      }
      isUDTF = (genericUDTF != null);
      if (isUDTF) {
        globalLimitCtx.setHasTransformOrUDTF(true);
      }
      if (isUDTF && !fi.isNative()) {
        unparseTranslator.addIdentifierTranslation((ASTNode) udtfExpr
            .getChild(0));
      }
      if (isUDTF && (selectStar = udtfExprType == HiveParser.TOK_FUNCTIONSTAR)) {
        genColListRegex(".*", null, (ASTNode) udtfExpr.getChild(0),
            col_list, null, inputRR, starRR, pos, out_rwsch, qb.getAliases(), false);
      }
    }

    if (isUDTF) {
      // Only support a single expression when it's a UDTF
      if (selExprList.getChildCount() > 1) {
        throw new SemanticException(generateErrorMessage(
            (ASTNode) selExprList.getChild(1),
            ErrorMsg.UDTF_MULTIPLE_EXPR.getMsg()));
      }

      ASTNode selExpr = (ASTNode) selExprList.getChild(posn);

      // Get the column / table aliases from the expression. Start from 1 as
      // 0 is the TOK_FUNCTION
      // column names also can be inferred from result of UDTF
      for (int i = 1; i < selExpr.getChildCount(); i++) {
        ASTNode selExprChild = (ASTNode) selExpr.getChild(i);
        switch (selExprChild.getType()) {
        case HiveParser.Identifier:
          udtfColAliases.add(unescapeIdentifier(selExprChild.getText()));
          unparseTranslator.addIdentifierTranslation(selExprChild);
          break;
        case HiveParser.TOK_TABALIAS:
          assert (selExprChild.getChildCount() == 1);
          udtfTableAlias = unescapeIdentifier(selExprChild.getChild(0)
              .getText());
          qb.addAlias(udtfTableAlias);
          unparseTranslator.addIdentifierTranslation((ASTNode) selExprChild
              .getChild(0));
          break;
        default:
          assert (false);
        }
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("UDTF table alias is " + udtfTableAlias);
        LOG.debug("UDTF col aliases are " + udtfColAliases);
      }
    }

    // The list of expressions after SELECT or SELECT TRANSFORM.
    ASTNode exprList;
    if (isInTransform) {
      exprList = (ASTNode) trfm.getChild(0);
    } else if (isUDTF) {
      exprList = udtfExpr;
    } else {
      exprList = selExprList;
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("genSelectPlan: input = " + inputRR + " starRr = " + starRR);
    }

    // For UDTF's, skip the function name to get the expressions
    int startPosn = isUDTF ? posn + 1 : posn;
    if (isInTransform) {
      startPosn = 0;
    }

    Set<String> colAliases = new HashSet<String>();
    ASTNode[] exprs = new ASTNode[exprList.getChildCount()];
    String[][] aliases = new String[exprList.getChildCount()][];
    boolean[] hasAsClauses = new boolean[exprList.getChildCount()];
    // Iterate over all expression (either after SELECT, or in SELECT TRANSFORM)
    for (int i = startPosn; i < exprList.getChildCount(); ++i) {

      // child can be EXPR AS ALIAS, or EXPR.
      ASTNode child = (ASTNode) exprList.getChild(i);
      boolean hasAsClause = (!isInTransform) && (child.getChildCount() == 2);
      boolean isWindowSpec = child.getChildCount() == 3 &&
          child.getChild(2).getType() == HiveParser.TOK_WINDOWSPEC;

      // EXPR AS (ALIAS,...) parses, but is only allowed for UDTF's
      // This check is not needed and invalid when there is a transform b/c the
      // AST's are slightly different.
      if (!isWindowSpec && !isInTransform && !isUDTF && child.getChildCount() > 2) {
        throw new SemanticException(generateErrorMessage(
            (ASTNode) child.getChild(2),
            ErrorMsg.INVALID_AS.getMsg()));
      }

      // The real expression
      ASTNode expr;
      String tabAlias;
      String colAlias;

      if (isInTransform || isUDTF) {
        tabAlias = null;
        colAlias = autogenColAliasPrfxLbl + i;
        expr = child;
      } else {
        // Get rid of TOK_SELEXPR
        expr = (ASTNode) child.getChild(0);
        String[] colRef = getColAlias(child, autogenColAliasPrfxLbl, inputRR,
            autogenColAliasPrfxIncludeFuncName, i);
        tabAlias = colRef[0];
        colAlias = colRef[1];
        if (hasAsClause) {
          unparseTranslator.addIdentifierTranslation((ASTNode) child
              .getChild(1));
        }
      }
      exprs[i] = expr;
      aliases[i] = new String[] {tabAlias, colAlias};
      hasAsClauses[i] = hasAsClause;
      colAliases.add(colAlias);
    }

    // Iterate over all expression (either after SELECT, or in SELECT TRANSFORM)
    for (int i = startPosn; i < exprList.getChildCount(); ++i) {
      // The real expression
      ASTNode expr = exprs[i];
      String tabAlias = aliases[i][0];
      String colAlias = aliases[i][1];
      boolean hasAsClause = hasAsClauses[i];

      if (expr.getType() == HiveParser.TOK_ALLCOLREF) {
        pos = genColListRegex(".*", expr.getChildCount() == 0 ? null
            : getUnescapedName((ASTNode) expr.getChild(0)).toLowerCase(),
            expr, col_list, null, inputRR, starRR, pos, out_rwsch, qb.getAliases(), false);
        selectStar = true;
      } else if (expr.getType() == HiveParser.TOK_TABLE_OR_COL && !hasAsClause
          && !inputRR.getIsExprResolver()
          && isRegex(unescapeIdentifier(expr.getChild(0).getText()), conf)) {
        // In case the expression is a regex COL.
        // This can only happen without AS clause
        // We don't allow this for ExprResolver - the Group By case
        pos = genColListRegex(unescapeIdentifier(expr.getChild(0).getText()),
            null, expr, col_list, null, inputRR, starRR, pos, out_rwsch, qb.getAliases(), false);
      } else if (expr.getType() == HiveParser.DOT
          && expr.getChild(0).getType() == HiveParser.TOK_TABLE_OR_COL
          && inputRR.hasTableAlias(unescapeIdentifier(expr.getChild(0)
              .getChild(0).getText().toLowerCase())) && !hasAsClause
          && !inputRR.getIsExprResolver()
          && isRegex(unescapeIdentifier(expr.getChild(1).getText()), conf)) {
        // In case the expression is TABLE.COL (col can be regex).
        // This can only happen without AS clause
        // We don't allow this for ExprResolver - the Group By case
        pos = genColListRegex(unescapeIdentifier(expr.getChild(1).getText()),
            unescapeIdentifier(expr.getChild(0).getChild(0).getText().toLowerCase()),
             expr, col_list, null, inputRR, starRR, pos, out_rwsch, qb.getAliases(), false);
      } else {
        // Case when this is an expression
        TypeCheckCtx tcCtx = new TypeCheckCtx(inputRR);
        // We allow stateful functions in the SELECT list (but nowhere else)
        tcCtx.setAllowStatefulFunctions(true);
        tcCtx.setAllowDistinctFunctions(false);
        ExprNodeDesc exp = genExprNodeDesc(expr, inputRR, tcCtx);
        String recommended = recommendName(exp, colAlias);
        if (recommended != null && !colAliases.contains(recommended) &&
            out_rwsch.get(null, recommended) == null) {
          colAlias = recommended;
        }
        col_list.add(exp);

        ColumnInfo colInfo = new ColumnInfo(getColumnInternalName(pos),
            exp.getWritableObjectInspector(), tabAlias, false);
        colInfo.setSkewedCol((exp instanceof ExprNodeColumnDesc) ? ((ExprNodeColumnDesc) exp)
            .isSkewedCol() : false);
        out_rwsch.put(tabAlias, colAlias, colInfo);

        if ( exp instanceof ExprNodeColumnDesc ) {
          ExprNodeColumnDesc colExp = (ExprNodeColumnDesc) exp;
          String[] altMapping = inputRR.getAlternateMappings(colExp.getColumn());
          if ( altMapping != null ) {
            out_rwsch.put(altMapping[0], altMapping[1], colInfo);
          }
        }

        pos = Integer.valueOf(pos.intValue() + 1);
      }
    }
    selectStar = selectStar && exprList.getChildCount() == posn + 1;

    ArrayList<String> columnNames = new ArrayList<String>();
    Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();
    for (int i = 0; i < col_list.size(); i++) {
      String outputCol = getColumnInternalName(i);
      colExprMap.put(outputCol, col_list.get(i));
      columnNames.add(outputCol);
    }

    Operator output = putOpInsertMap(OperatorFactory.getAndMakeChild(
        new SelectDesc(col_list, columnNames, selectStar), new RowSchema(
            out_rwsch.getColumnInfos()), input), out_rwsch);

    output.setColumnExprMap(colExprMap);
    if (isInTransform) {
      output = genScriptPlan(trfm, qb, output);
    }

    if (isUDTF) {
      output = genUDTFPlan(genericUDTF, udtfTableAlias, udtfColAliases, qb,
          output, outerLV);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Created Select Plan row schema: " + out_rwsch.toString());
    }
    return output;
  }

  String recommendName(ExprNodeDesc exp, String colAlias) {
    if (!colAlias.startsWith(autogenColAliasPrfxLbl)) {
      return null;
    }
    String column = ExprNodeDescUtils.recommendInputName(exp);
    if (column != null && !column.startsWith(autogenColAliasPrfxLbl)) {
      return column;
    }
    return null;
  }

  String getAutogenColAliasPrfxLbl() {
    return this.autogenColAliasPrfxLbl;
  }

  boolean autogenColAliasPrfxIncludeFuncName() {
    return this.autogenColAliasPrfxIncludeFuncName;
  }

  /**
   * Class to store GenericUDAF related information.
   */
  static class GenericUDAFInfo {
    ArrayList<ExprNodeDesc> convertedParameters;
    GenericUDAFEvaluator genericUDAFEvaluator;
    TypeInfo returnType;
  }

  /**
   * Convert exprNodeDesc array to Typeinfo array.
   */
  static ArrayList<TypeInfo> getTypeInfo(ArrayList<ExprNodeDesc> exprs) {
    ArrayList<TypeInfo> result = new ArrayList<TypeInfo>();
    for (ExprNodeDesc expr : exprs) {
      result.add(expr.getTypeInfo());
    }
    return result;
  }

  /**
   * Convert exprNodeDesc array to ObjectInspector array.
   */
  static ArrayList<ObjectInspector> getWritableObjectInspector(ArrayList<ExprNodeDesc> exprs) {
    ArrayList<ObjectInspector> result = new ArrayList<ObjectInspector>();
    for (ExprNodeDesc expr : exprs) {
      result.add(expr.getWritableObjectInspector());
    }
    return result;
  }

  /**
   * Convert exprNodeDesc array to Typeinfo array.
   */
  static ObjectInspector[] getStandardObjectInspector(ArrayList<TypeInfo> exprs) {
    ObjectInspector[] result = new ObjectInspector[exprs.size()];
    for (int i = 0; i < exprs.size(); i++) {
      result[i] = TypeInfoUtils
          .getStandardWritableObjectInspectorFromTypeInfo(exprs.get(i));
    }
    return result;
  }

  /**
   * Returns the GenericUDAFEvaluator for the aggregation. This is called once
   * for each GroupBy aggregation.
   */
  static GenericUDAFEvaluator getGenericUDAFEvaluator(String aggName,
      ArrayList<ExprNodeDesc> aggParameters, ASTNode aggTree,
      boolean isDistinct, boolean isAllColumns)
      throws SemanticException {
    ArrayList<ObjectInspector> originalParameterTypeInfos =
        getWritableObjectInspector(aggParameters);
    GenericUDAFEvaluator result = FunctionRegistry.getGenericUDAFEvaluator(
        aggName, originalParameterTypeInfos, isDistinct, isAllColumns);
    if (null == result) {
      String reason = "Looking for UDAF Evaluator\"" + aggName
          + "\" with parameters " + originalParameterTypeInfos;
      throw new SemanticException(ErrorMsg.INVALID_FUNCTION_SIGNATURE.getMsg(
          (ASTNode) aggTree.getChild(0), reason));
    }
    return result;
  }

  /**
   * Returns the GenericUDAFInfo struct for the aggregation.
   *
   * @param aggName
   *          The name of the UDAF.
   * @param aggParameters
   *          The exprNodeDesc of the original parameters
   * @param aggTree
   *          The ASTNode node of the UDAF in the query.
   * @return GenericUDAFInfo
   * @throws SemanticException
   *           when the UDAF is not found or has problems.
   */
  static GenericUDAFInfo getGenericUDAFInfo(GenericUDAFEvaluator evaluator,
      GenericUDAFEvaluator.Mode emode, ArrayList<ExprNodeDesc> aggParameters)
      throws SemanticException {

    GenericUDAFInfo r = new GenericUDAFInfo();

    // set r.genericUDAFEvaluator
    r.genericUDAFEvaluator = evaluator;

    // set r.returnType
    ObjectInspector returnOI = null;
    try {
      ArrayList<ObjectInspector> aggOIs = getWritableObjectInspector(aggParameters);
      ObjectInspector[] aggOIArray = new ObjectInspector[aggOIs.size()];
      for (int ii = 0; ii < aggOIs.size(); ++ii) {
        aggOIArray[ii] = aggOIs.get(ii);
      }
      returnOI = r.genericUDAFEvaluator.init(emode, aggOIArray);
      r.returnType = TypeInfoUtils.getTypeInfoFromObjectInspector(returnOI);
    } catch (HiveException e) {
      throw new SemanticException(e);
    }
    // set r.convertedParameters
    // TODO: type conversion
    r.convertedParameters = aggParameters;

    return r;
  }

  static GenericUDAFEvaluator.Mode groupByDescModeToUDAFMode(
      GroupByDesc.Mode mode, boolean isDistinct) {
    switch (mode) {
    case COMPLETE:
      return GenericUDAFEvaluator.Mode.COMPLETE;
    case PARTIAL1:
      return GenericUDAFEvaluator.Mode.PARTIAL1;
    case PARTIAL2:
      return GenericUDAFEvaluator.Mode.PARTIAL2;
    case PARTIALS:
      return isDistinct ? GenericUDAFEvaluator.Mode.PARTIAL1
          : GenericUDAFEvaluator.Mode.PARTIAL2;
    case FINAL:
      return GenericUDAFEvaluator.Mode.FINAL;
    case HASH:
      return GenericUDAFEvaluator.Mode.PARTIAL1;
    case MERGEPARTIAL:
      return isDistinct ? GenericUDAFEvaluator.Mode.COMPLETE
          : GenericUDAFEvaluator.Mode.FINAL;
    default:
      throw new RuntimeException("internal error in groupByDescModeToUDAFMode");
    }
  }

  /**
   * Check if the given internalName represents a constant parameter in aggregation parameters
   * of an aggregation tree.
   * This method is only invoked when map-side aggregation is not involved. In this case,
   * every parameter in every aggregation tree should already have a corresponding ColumnInfo,
   * which is generated when the corresponding ReduceSinkOperator of the GroupByOperator being
   * generating is generated. If we find that this parameter is a constant parameter,
   * we will return the corresponding ExprNodeDesc in reduceValues, and we will not need to
   * use a new ExprNodeColumnDesc, which can not be treated as a constant parameter, for this
   * parameter (since the writableObjectInspector of a ExprNodeColumnDesc will not be
   * a instance of ConstantObjectInspector).
   *
   * @param reduceValues
   *          value columns of the corresponding ReduceSinkOperator
   * @param internalName
   *          the internal name of this parameter
   * @return the ExprNodeDesc of the constant parameter if the given internalName represents
   *         a constant parameter; otherwise, return null
   */
  private ExprNodeDesc isConstantParameterInAggregationParameters(String internalName,
      List<ExprNodeDesc> reduceValues) {
    // only the pattern of "VALUE._col([0-9]+)" should be handled.

    String[] terms = internalName.split("\\.");
    if (terms.length != 2 || reduceValues == null) {
      return null;
    }

    if (Utilities.ReduceField.VALUE.toString().equals(terms[0])) {
      int pos = getPositionFromInternalName(terms[1]);
      if (pos >= 0 && pos < reduceValues.size()) {
        ExprNodeDesc reduceValue = reduceValues.get(pos);
        if (reduceValue != null) {
          if (reduceValue.getWritableObjectInspector() instanceof ConstantObjectInspector) {
            // this internalName represents a constant parameter in aggregation parameters
            return reduceValue;
          }
        }
      }
    }

    return null;
  }

  /**
   * Generate the GroupByOperator for the Query Block (parseInfo.getXXX(dest)).
   * The new GroupByOperator will be a child of the reduceSinkOperatorInfo.
   *
   * @param mode
   *          The mode of the aggregation (PARTIAL1 or COMPLETE)
   * @param genericUDAFEvaluators
   *          If not null, this function will store the mapping from Aggregation
   *          StringTree to the genericUDAFEvaluator in this parameter, so it
   *          can be used in the next-stage GroupBy aggregations.
   * @return the new GroupByOperator
   */
  @SuppressWarnings("nls")
  private Operator genGroupByPlanGroupByOperator(QBParseInfo parseInfo,
      String dest, Operator input, ReduceSinkOperator rs, GroupByDesc.Mode mode,
      Map<String, GenericUDAFEvaluator> genericUDAFEvaluators)
      throws SemanticException {
    RowResolver groupByInputRowResolver = opParseCtx
        .get(input).getRowResolver();
    RowResolver groupByOutputRowResolver = new RowResolver();
    groupByOutputRowResolver.setIsExprResolver(true);
    ArrayList<ExprNodeDesc> groupByKeys = new ArrayList<ExprNodeDesc>();
    ArrayList<AggregationDesc> aggregations = new ArrayList<AggregationDesc>();
    ArrayList<String> outputColumnNames = new ArrayList<String>();
    Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();
    List<ASTNode> grpByExprs = getGroupByForClause(parseInfo, dest);
    for (int i = 0; i < grpByExprs.size(); ++i) {
      ASTNode grpbyExpr = grpByExprs.get(i);
      ColumnInfo exprInfo = groupByInputRowResolver.getExpression(grpbyExpr);

      if (exprInfo == null) {
        throw new SemanticException(ErrorMsg.INVALID_COLUMN.getMsg(grpbyExpr));
      }

      groupByKeys.add(new ExprNodeColumnDesc(exprInfo.getType(), exprInfo
          .getInternalName(), "", false));
      String field = getColumnInternalName(i);
      outputColumnNames.add(field);
      ColumnInfo oColInfo = new ColumnInfo(field, exprInfo.getType(), null, false);
      groupByOutputRowResolver.putExpression(grpbyExpr,
          oColInfo);
      addAlternateGByKeyMappings(grpbyExpr, oColInfo, input, groupByOutputRowResolver);
      colExprMap.put(field, groupByKeys.get(groupByKeys.size() - 1));
    }
    // For each aggregation
    HashMap<String, ASTNode> aggregationTrees = parseInfo
        .getAggregationExprsForClause(dest);
    assert (aggregationTrees != null);
    // get the last colName for the reduce KEY
    // it represents the column name corresponding to distinct aggr, if any
    String lastKeyColName = null;
    List<String> inputKeyCols = rs.getConf().getOutputKeyColumnNames();
    if (inputKeyCols.size() > 0) {
      lastKeyColName = inputKeyCols.get(inputKeyCols.size() - 1);
    }
    List<ExprNodeDesc> reduceValues = rs.getConf().getValueCols();
    int numDistinctUDFs = 0;
    for (Map.Entry<String, ASTNode> entry : aggregationTrees.entrySet()) {
      ASTNode value = entry.getValue();

      // This is the GenericUDAF name
      String aggName = unescapeIdentifier(value.getChild(0).getText());
      boolean isDistinct = value.getType() == HiveParser.TOK_FUNCTIONDI;
      boolean isAllColumns = value.getType() == HiveParser.TOK_FUNCTIONSTAR;

      // Convert children to aggParameters
      ArrayList<ExprNodeDesc> aggParameters = new ArrayList<ExprNodeDesc>();
      // 0 is the function name
      for (int i = 1; i < value.getChildCount(); i++) {
        ASTNode paraExpr = (ASTNode) value.getChild(i);
        ColumnInfo paraExprInfo =
            groupByInputRowResolver.getExpression(paraExpr);
        if (paraExprInfo == null) {
          throw new SemanticException(ErrorMsg.INVALID_COLUMN.getMsg(paraExpr));
        }

        String paraExpression = paraExprInfo.getInternalName();
        assert (paraExpression != null);
        if (isDistinct && lastKeyColName != null) {
          // if aggr is distinct, the parameter is name is constructed as
          // KEY.lastKeyColName:<tag>._colx
          paraExpression = Utilities.ReduceField.KEY.name() + "." +
              lastKeyColName + ":" + numDistinctUDFs + "." +
              getColumnInternalName(i - 1);

        }

        ExprNodeDesc expr = new ExprNodeColumnDesc(paraExprInfo.getType(),
            paraExpression, paraExprInfo.getTabAlias(),
            paraExprInfo.getIsVirtualCol());
        ExprNodeDesc reduceValue = isConstantParameterInAggregationParameters(
            paraExprInfo.getInternalName(), reduceValues);

        if (reduceValue != null) {
          // this parameter is a constant
          expr = reduceValue;
        }

        aggParameters.add(expr);
      }

      if (isDistinct) {
        numDistinctUDFs++;
      }
      Mode amode = groupByDescModeToUDAFMode(mode, isDistinct);
      GenericUDAFEvaluator genericUDAFEvaluator = getGenericUDAFEvaluator(
          aggName, aggParameters, value, isDistinct, isAllColumns);
      assert (genericUDAFEvaluator != null);
      GenericUDAFInfo udaf = getGenericUDAFInfo(genericUDAFEvaluator, amode,
          aggParameters);
      aggregations.add(new AggregationDesc(aggName.toLowerCase(),
          udaf.genericUDAFEvaluator, udaf.convertedParameters, isDistinct,
          amode));
      String field = getColumnInternalName(groupByKeys.size()
          + aggregations.size() - 1);
      outputColumnNames.add(field);
      groupByOutputRowResolver.putExpression(value, new ColumnInfo(
          field, udaf.returnType, "", false));
      // Save the evaluator so that it can be used by the next-stage
      // GroupByOperators
      if (genericUDAFEvaluators != null) {
        genericUDAFEvaluators.put(entry.getKey(), genericUDAFEvaluator);
      }
    }
    float groupByMemoryUsage = HiveConf.getFloatVar(conf, HiveConf.ConfVars.HIVEMAPAGGRHASHMEMORY);
    float memoryThreshold = HiveConf
        .getFloatVar(conf, HiveConf.ConfVars.HIVEMAPAGGRMEMORYTHRESHOLD);

    Operator op = putOpInsertMap(OperatorFactory.getAndMakeChild(
        new GroupByDesc(mode, outputColumnNames, groupByKeys, aggregations,
            false, groupByMemoryUsage, memoryThreshold, null, false, -1, numDistinctUDFs > 0),
        new RowSchema(groupByOutputRowResolver.getColumnInfos()),
        input), groupByOutputRowResolver);
    op.setColumnExprMap(colExprMap);
    return op;
  }

  // Add the grouping set key to the group by operator.
  // This is not the first group by operator, but it is a subsequent group by operator
  // which is forwarding the grouping keys introduced by the grouping sets.
  // For eg: consider: select key, value, count(1) from T group by key, value with rollup.
  // Assuming map-side aggregation and no skew, the plan would look like:
  //
  // TableScan --> Select --> GroupBy1 --> ReduceSink --> GroupBy2 --> Select --> FileSink
  //
  // This function is called for GroupBy2 to pass the additional grouping keys introduced by
  // GroupBy1 for the grouping set (corresponding to the rollup).
  private void addGroupingSetKey(List<ExprNodeDesc> groupByKeys,
      RowResolver groupByInputRowResolver,
      RowResolver groupByOutputRowResolver,
      List<String> outputColumnNames,
      Map<String, ExprNodeDesc> colExprMap) throws SemanticException {
    // For grouping sets, add a dummy grouping key
    String groupingSetColumnName =
        groupByInputRowResolver.get(null, VirtualColumn.GROUPINGID.getName()).getInternalName();
    ExprNodeDesc inputExpr = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo,
        groupingSetColumnName, null, false);
    groupByKeys.add(inputExpr);

    String field = getColumnInternalName(groupByKeys.size() - 1);
    outputColumnNames.add(field);
    groupByOutputRowResolver.put(null, VirtualColumn.GROUPINGID.getName(),
        new ColumnInfo(
            field,
            TypeInfoFactory.stringTypeInfo,
            null,
            true));
    colExprMap.put(field, groupByKeys.get(groupByKeys.size() - 1));
  }

  // Process grouping set for the reduce sink operator
  // For eg: consider: select key, value, count(1) from T group by key, value with rollup.
  // Assuming map-side aggregation and no skew, the plan would look like:
  //
  // TableScan --> Select --> GroupBy1 --> ReduceSink --> GroupBy2 --> Select --> FileSink
  //
  // This function is called for ReduceSink to add the additional grouping keys introduced by
  // GroupBy1 into the reduce keys.
  private void processGroupingSetReduceSinkOperator(RowResolver reduceSinkInputRowResolver,
      RowResolver reduceSinkOutputRowResolver,
      List<ExprNodeDesc> reduceKeys,
      List<String> outputKeyColumnNames,
      Map<String, ExprNodeDesc> colExprMap) throws SemanticException {
    // add a key for reduce sink
    String groupingSetColumnName =
        reduceSinkInputRowResolver.get(null, VirtualColumn.GROUPINGID.getName()).getInternalName();
    ExprNodeDesc inputExpr = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo,
        groupingSetColumnName, null, false);
    reduceKeys.add(inputExpr);

    outputKeyColumnNames.add(getColumnInternalName(reduceKeys.size() - 1));
    String field = Utilities.ReduceField.KEY.toString() + "."
        + getColumnInternalName(reduceKeys.size() - 1);
    ColumnInfo colInfo = new ColumnInfo(field, reduceKeys.get(
        reduceKeys.size() - 1).getTypeInfo(), null, true);
    reduceSinkOutputRowResolver.put(null, VirtualColumn.GROUPINGID.getName(), colInfo);
    colExprMap.put(colInfo.getInternalName(), inputExpr);
  }


  /**
   * Generate the GroupByOperator for the Query Block (parseInfo.getXXX(dest)).
   * The new GroupByOperator will be a child of the reduceSinkOperatorInfo.
   *
   * @param mode
   *          The mode of the aggregation (MERGEPARTIAL, PARTIAL2)
   * @param genericUDAFEvaluators
   *          The mapping from Aggregation StringTree to the
   *          genericUDAFEvaluator.
   * @param distPartAggr
   *          partial aggregation for distincts
   * @param groupingSets
   *          list of grouping sets
   * @param groupingSetsPresent
   *          whether grouping sets are present in this query
   * @param groupingSetsConsumedCurrentMR
   *          whether grouping sets are consumed by this group by
   * @return the new GroupByOperator
   */
  @SuppressWarnings("nls")
  private Operator genGroupByPlanGroupByOperator1(QBParseInfo parseInfo,
      String dest, Operator reduceSinkOperatorInfo, GroupByDesc.Mode mode,
      Map<String, GenericUDAFEvaluator> genericUDAFEvaluators,
      boolean distPartAgg,
      List<Integer> groupingSets,
      boolean groupingSetsPresent,
      boolean groupingSetsNeedAdditionalMRJob) throws SemanticException {
    ArrayList<String> outputColumnNames = new ArrayList<String>();
    RowResolver groupByInputRowResolver = opParseCtx
        .get(reduceSinkOperatorInfo).getRowResolver();
    RowResolver groupByOutputRowResolver = new RowResolver();
    groupByOutputRowResolver.setIsExprResolver(true);
    ArrayList<ExprNodeDesc> groupByKeys = new ArrayList<ExprNodeDesc>();
    ArrayList<AggregationDesc> aggregations = new ArrayList<AggregationDesc>();
    List<ASTNode> grpByExprs = getGroupByForClause(parseInfo, dest);
    Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();
    for (int i = 0; i < grpByExprs.size(); ++i) {
      ASTNode grpbyExpr = grpByExprs.get(i);
      ColumnInfo exprInfo = groupByInputRowResolver.getExpression(grpbyExpr);

      if (exprInfo == null) {
        throw new SemanticException(ErrorMsg.INVALID_COLUMN.getMsg(grpbyExpr));
      }

      groupByKeys.add(new ExprNodeColumnDesc(exprInfo));
      String field = getColumnInternalName(i);
      outputColumnNames.add(field);
      ColumnInfo oColInfo = new ColumnInfo(field, exprInfo.getType(), "", false);
      groupByOutputRowResolver.putExpression(grpbyExpr,
          oColInfo);
      addAlternateGByKeyMappings(grpbyExpr, oColInfo, reduceSinkOperatorInfo, groupByOutputRowResolver);
      colExprMap.put(field, groupByKeys.get(groupByKeys.size() - 1));
    }

    // This is only needed if a new grouping set key is being created
    int groupingSetsPosition = -1;

    // For grouping sets, add a dummy grouping key
    if (groupingSetsPresent) {
      groupingSetsPosition = groupByKeys.size();
      // Consider the query: select a,b, count(1) from T group by a,b with cube;
      // where it is being executed in a single map-reduce job
      // The plan is TableScan -> GroupBy1 -> ReduceSink -> GroupBy2 -> FileSink
      // GroupBy1 already added the grouping id as part of the row
      // This function is called for GroupBy2 to add grouping id as part of the groupby keys
      if (!groupingSetsNeedAdditionalMRJob) {
        addGroupingSetKey(
            groupByKeys,
            groupByInputRowResolver,
            groupByOutputRowResolver,
            outputColumnNames,
            colExprMap);
      }
      else {
        // The grouping set has not yet been processed. Create a new grouping key
        // Consider the query: select a,b, count(1) from T group by a,b with cube;
        // where it is being executed in 2 map-reduce jobs
        // The plan for 1st MR is TableScan -> GroupBy1 -> ReduceSink -> GroupBy2 -> FileSink
        // GroupBy1/ReduceSink worked as if grouping sets were not present
        // This function is called for GroupBy2 to create new rows for grouping sets
        // For each input row (a,b), 4 rows are created for the example above:
        // (a,b), (a,null), (null, b), (null, null)
        createNewGroupingKey(groupByKeys,
            outputColumnNames,
            groupByOutputRowResolver,
            colExprMap);
      }
    }

    HashMap<String, ASTNode> aggregationTrees = parseInfo
        .getAggregationExprsForClause(dest);
    // get the last colName for the reduce KEY
    // it represents the column name corresponding to distinct aggr, if any
    String lastKeyColName = null;
    List<ExprNodeDesc> reduceValues = null;
    if (reduceSinkOperatorInfo.getConf() instanceof ReduceSinkDesc) {
      List<String> inputKeyCols = ((ReduceSinkDesc)
          reduceSinkOperatorInfo.getConf()).getOutputKeyColumnNames();
      if (inputKeyCols.size() > 0) {
        lastKeyColName = inputKeyCols.get(inputKeyCols.size() - 1);
      }
      reduceValues = ((ReduceSinkDesc) reduceSinkOperatorInfo.getConf()).getValueCols();
    }
    int numDistinctUDFs = 0;
    boolean containsDistinctAggr = false;
    for (Map.Entry<String, ASTNode> entry : aggregationTrees.entrySet()) {
      ASTNode value = entry.getValue();
      String aggName = unescapeIdentifier(value.getChild(0).getText());
      ArrayList<ExprNodeDesc> aggParameters = new ArrayList<ExprNodeDesc>();
      boolean isDistinct = (value.getType() == HiveParser.TOK_FUNCTIONDI);
      containsDistinctAggr = containsDistinctAggr || isDistinct;

      // If the function is distinct, partial aggregation has not been done on
      // the client side.
      // If distPartAgg is set, the client is letting us know that partial
      // aggregation has not been done.
      // For eg: select a, count(b+c), count(distinct d+e) group by a
      // For count(b+c), if partial aggregation has been performed, then we
      // directly look for count(b+c).
      // Otherwise, we look for b+c.
      // For distincts, partial aggregation is never performed on the client
      // side, so always look for the parameters: d+e
      boolean partialAggDone = !(distPartAgg || isDistinct);
      if (!partialAggDone) {
        // 0 is the function name
        for (int i = 1; i < value.getChildCount(); i++) {
          ASTNode paraExpr = (ASTNode) value.getChild(i);
          ColumnInfo paraExprInfo =
              groupByInputRowResolver.getExpression(paraExpr);
          if (paraExprInfo == null) {
            throw new SemanticException(ErrorMsg.INVALID_COLUMN
                .getMsg(paraExpr));
          }

          String paraExpression = paraExprInfo.getInternalName();
          assert (paraExpression != null);
          if (isDistinct && lastKeyColName != null) {
            // if aggr is distinct, the parameter is name is constructed as
            // KEY.lastKeyColName:<tag>._colx
            paraExpression = Utilities.ReduceField.KEY.name() + "." +
                lastKeyColName + ":" + numDistinctUDFs + "."
                + getColumnInternalName(i - 1);

          }

          ExprNodeDesc expr = new ExprNodeColumnDesc(paraExprInfo.getType(),
              paraExpression, paraExprInfo.getTabAlias(),
              paraExprInfo.getIsVirtualCol());
          ExprNodeDesc reduceValue = isConstantParameterInAggregationParameters(
              paraExprInfo.getInternalName(), reduceValues);

          if (reduceValue != null) {
            // this parameter is a constant
            expr = reduceValue;
          }

          aggParameters.add(expr);

        }
      } else {
        ColumnInfo paraExprInfo = groupByInputRowResolver.getExpression(value);
        if (paraExprInfo == null) {
          throw new SemanticException(ErrorMsg.INVALID_COLUMN.getMsg(value));
        }
        String paraExpression = paraExprInfo.getInternalName();
        assert (paraExpression != null);
        aggParameters.add(new ExprNodeColumnDesc(paraExprInfo.getType(),
            paraExpression, paraExprInfo.getTabAlias(), paraExprInfo
                .getIsVirtualCol()));
      }
      if (isDistinct) {
        numDistinctUDFs++;
      }
      boolean isAllColumns = value.getType() == HiveParser.TOK_FUNCTIONSTAR;
      Mode amode = groupByDescModeToUDAFMode(mode, isDistinct);
      GenericUDAFEvaluator genericUDAFEvaluator = null;
      // For distincts, partial aggregations have not been done
      if (distPartAgg) {
        genericUDAFEvaluator = getGenericUDAFEvaluator(aggName, aggParameters,
            value, isDistinct, isAllColumns);
        assert (genericUDAFEvaluator != null);
        genericUDAFEvaluators.put(entry.getKey(), genericUDAFEvaluator);
      } else {
        genericUDAFEvaluator = genericUDAFEvaluators.get(entry.getKey());
        assert (genericUDAFEvaluator != null);
      }

      GenericUDAFInfo udaf = getGenericUDAFInfo(genericUDAFEvaluator, amode,
          aggParameters);
      aggregations.add(new AggregationDesc(aggName.toLowerCase(),
          udaf.genericUDAFEvaluator, udaf.convertedParameters,
          (mode != GroupByDesc.Mode.FINAL && isDistinct), amode));
      String field = getColumnInternalName(groupByKeys.size()
          + aggregations.size() - 1);
      outputColumnNames.add(field);
      groupByOutputRowResolver.putExpression(value, new ColumnInfo(
          field, udaf.returnType, "", false));
    }
    float groupByMemoryUsage = HiveConf.getFloatVar(conf, HiveConf.ConfVars.HIVEMAPAGGRHASHMEMORY);
    float memoryThreshold = HiveConf
        .getFloatVar(conf, HiveConf.ConfVars.HIVEMAPAGGRMEMORYTHRESHOLD);

    // Nothing special needs to be done for grouping sets if
    // this is the final group by operator, and multiple rows corresponding to the
    // grouping sets have been generated upstream.
    // However, if an addition MR job has been created to handle grouping sets,
    // additional rows corresponding to grouping sets need to be created here.
    Operator op = putOpInsertMap(OperatorFactory.getAndMakeChild(
        new GroupByDesc(mode, outputColumnNames, groupByKeys, aggregations,
            distPartAgg, groupByMemoryUsage, memoryThreshold,
            groupingSets,
            groupingSetsPresent && groupingSetsNeedAdditionalMRJob,
            groupingSetsPosition, containsDistinctAggr),
        new RowSchema(groupByOutputRowResolver.getColumnInfos()), reduceSinkOperatorInfo),
        groupByOutputRowResolver);
    op.setColumnExprMap(colExprMap);
    return op;
  }

  /*
   * Create a new grouping key for grouping id.
   * A dummy grouping id. is added. At runtime, the group by operator
   * creates 'n' rows per input row, where 'n' is the number of grouping sets.
   */
  private void createNewGroupingKey(List<ExprNodeDesc> groupByKeys,
      List<String> outputColumnNames,
      RowResolver groupByOutputRowResolver,
      Map<String, ExprNodeDesc> colExprMap) {
    // The value for the constant does not matter. It is replaced by the grouping set
    // value for the actual implementation
    ExprNodeConstantDesc constant = new ExprNodeConstantDesc("0");
    groupByKeys.add(constant);
    String field = getColumnInternalName(groupByKeys.size() - 1);
    outputColumnNames.add(field);
    groupByOutputRowResolver.put(null, VirtualColumn.GROUPINGID.getName(),
        new ColumnInfo(
            field,
            TypeInfoFactory.stringTypeInfo,
            null,
            true));
    colExprMap.put(field, constant);
  }

  /**
   * Generate the map-side GroupByOperator for the Query Block
   * (qb.getParseInfo().getXXX(dest)). The new GroupByOperator will be a child
   * of the inputOperatorInfo.
   *
   * @param mode
   *          The mode of the aggregation (HASH)
   * @param genericUDAFEvaluators
   *          If not null, this function will store the mapping from Aggregation
   *          StringTree to the genericUDAFEvaluator in this parameter, so it
   *          can be used in the next-stage GroupBy aggregations.
   * @return the new GroupByOperator
   */
  @SuppressWarnings("nls")
  private Operator genGroupByPlanMapGroupByOperator(QB qb,
      String dest,
      List<ASTNode> grpByExprs,
      Operator inputOperatorInfo,
      GroupByDesc.Mode mode,
      Map<String, GenericUDAFEvaluator> genericUDAFEvaluators,
      List<Integer> groupingSetKeys,
      boolean groupingSetsPresent) throws SemanticException {

    RowResolver groupByInputRowResolver = opParseCtx.get(inputOperatorInfo)
        .getRowResolver();
    QBParseInfo parseInfo = qb.getParseInfo();
    RowResolver groupByOutputRowResolver = new RowResolver();
    groupByOutputRowResolver.setIsExprResolver(true);
    ArrayList<ExprNodeDesc> groupByKeys = new ArrayList<ExprNodeDesc>();
    ArrayList<String> outputColumnNames = new ArrayList<String>();
    ArrayList<AggregationDesc> aggregations = new ArrayList<AggregationDesc>();
    Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();
    for (int i = 0; i < grpByExprs.size(); ++i) {
      ASTNode grpbyExpr = grpByExprs.get(i);
      ExprNodeDesc grpByExprNode = genExprNodeDesc(grpbyExpr,
          groupByInputRowResolver);

      groupByKeys.add(grpByExprNode);
      String field = getColumnInternalName(i);
      outputColumnNames.add(field);
      groupByOutputRowResolver.putExpression(grpbyExpr,
          new ColumnInfo(field, grpByExprNode.getTypeInfo(), "", false));
      colExprMap.put(field, groupByKeys.get(groupByKeys.size() - 1));
    }

    // The grouping set key is present after the grouping keys, before the distinct keys
    int groupingSetsPosition = -1;

    // For grouping sets, add a dummy grouping key
    // This dummy key needs to be added as a reduce key
    // For eg: consider: select key, value, count(1) from T group by key, value with rollup.
    // Assuming map-side aggregation and no skew, the plan would look like:
    //
    // TableScan --> Select --> GroupBy1 --> ReduceSink --> GroupBy2 --> Select --> FileSink
    //
    // This function is called for GroupBy1 to create an additional grouping key
    // for the grouping set (corresponding to the rollup).
    if (groupingSetsPresent) {
      groupingSetsPosition = groupByKeys.size();
      createNewGroupingKey(groupByKeys,
          outputColumnNames,
          groupByOutputRowResolver,
          colExprMap);
    }

    // If there is a distinctFuncExp, add all parameters to the reduceKeys.
    if (!parseInfo.getDistinctFuncExprsForClause(dest).isEmpty()) {
      List<ASTNode> list = parseInfo.getDistinctFuncExprsForClause(dest);
      for (ASTNode value : list) {
        // 0 is function name
        for (int i = 1; i < value.getChildCount(); i++) {
          ASTNode parameter = (ASTNode) value.getChild(i);
          if (groupByOutputRowResolver.getExpression(parameter) == null) {
            ExprNodeDesc distExprNode = genExprNodeDesc(parameter,
                groupByInputRowResolver);
            groupByKeys.add(distExprNode);
            String field = getColumnInternalName(groupByKeys.size() - 1);
            outputColumnNames.add(field);
            groupByOutputRowResolver.putExpression(parameter, new ColumnInfo(
                field, distExprNode.getTypeInfo(), "", false));
            colExprMap.put(field, groupByKeys.get(groupByKeys.size() - 1));
          }
        }
      }
    }

    // For each aggregation
    HashMap<String, ASTNode> aggregationTrees = parseInfo
        .getAggregationExprsForClause(dest);
    assert (aggregationTrees != null);

    boolean containsDistinctAggr = false;
    for (Map.Entry<String, ASTNode> entry : aggregationTrees.entrySet()) {
      ASTNode value = entry.getValue();
      String aggName = unescapeIdentifier(value.getChild(0).getText());
      ArrayList<ExprNodeDesc> aggParameters = new ArrayList<ExprNodeDesc>();
      // 0 is the function name
      for (int i = 1; i < value.getChildCount(); i++) {
        ASTNode paraExpr = (ASTNode) value.getChild(i);
        ExprNodeDesc paraExprNode = genExprNodeDesc(paraExpr,
            groupByInputRowResolver);

        aggParameters.add(paraExprNode);
      }

      boolean isDistinct = value.getType() == HiveParser.TOK_FUNCTIONDI;
      containsDistinctAggr = containsDistinctAggr || isDistinct;
      boolean isAllColumns = value.getType() == HiveParser.TOK_FUNCTIONSTAR;
      Mode amode = groupByDescModeToUDAFMode(mode, isDistinct);

      GenericUDAFEvaluator genericUDAFEvaluator = getGenericUDAFEvaluator(
          aggName, aggParameters, value, isDistinct, isAllColumns);
      assert (genericUDAFEvaluator != null);
      GenericUDAFInfo udaf = getGenericUDAFInfo(genericUDAFEvaluator, amode,
          aggParameters);
      aggregations.add(new AggregationDesc(aggName.toLowerCase(),
          udaf.genericUDAFEvaluator, udaf.convertedParameters, isDistinct,
          amode));
      String field = getColumnInternalName(groupByKeys.size()
          + aggregations.size() - 1);
      outputColumnNames.add(field);
      if (groupByOutputRowResolver.getExpression(value) == null) {
        groupByOutputRowResolver.putExpression(value, new ColumnInfo(
            field, udaf.returnType, "", false));
      }
      // Save the evaluator so that it can be used by the next-stage
      // GroupByOperators
      if (genericUDAFEvaluators != null) {
        genericUDAFEvaluators.put(entry.getKey(), genericUDAFEvaluator);
      }
    }
    float groupByMemoryUsage = HiveConf.getFloatVar(conf, HiveConf.ConfVars.HIVEMAPAGGRHASHMEMORY);
    float memoryThreshold = HiveConf
        .getFloatVar(conf, HiveConf.ConfVars.HIVEMAPAGGRMEMORYTHRESHOLD);
    Operator op = putOpInsertMap(OperatorFactory.getAndMakeChild(
        new GroupByDesc(mode, outputColumnNames, groupByKeys, aggregations,
            false, groupByMemoryUsage, memoryThreshold,
            groupingSetKeys, groupingSetsPresent, groupingSetsPosition, containsDistinctAggr),
        new RowSchema(groupByOutputRowResolver.getColumnInfos()),
        inputOperatorInfo), groupByOutputRowResolver);
    op.setColumnExprMap(colExprMap);
    return op;
  }

  /**
   * Generate the ReduceSinkOperator for the Group By Query Block
   * (qb.getPartInfo().getXXX(dest)). The new ReduceSinkOperator will be a child
   * of inputOperatorInfo.
   *
   * It will put all Group By keys and the distinct field (if any) in the
   * map-reduce sort key, and all other fields in the map-reduce value.
   *
   * @param numPartitionFields
   *          the number of fields for map-reduce partitioning. This is usually
   *          the number of fields in the Group By keys.
   * @return the new ReduceSinkOperator.
   * @throws SemanticException
   */
  @SuppressWarnings("nls")
  private ReduceSinkOperator genGroupByPlanReduceSinkOperator(QB qb,
      String dest,
      Operator inputOperatorInfo,
      List<ASTNode> grpByExprs,
      int numPartitionFields,
      boolean changeNumPartitionFields,
      int numReducers,
      boolean mapAggrDone,
      boolean groupingSetsPresent) throws SemanticException {

    RowResolver reduceSinkInputRowResolver = opParseCtx.get(inputOperatorInfo)
        .getRowResolver();
    QBParseInfo parseInfo = qb.getParseInfo();
    RowResolver reduceSinkOutputRowResolver = new RowResolver();
    reduceSinkOutputRowResolver.setIsExprResolver(true);
    Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();
    // Pre-compute group-by keys and store in reduceKeys

    List<String> outputKeyColumnNames = new ArrayList<String>();
    List<String> outputValueColumnNames = new ArrayList<String>();

    ArrayList<ExprNodeDesc> reduceKeys = getReduceKeysForReduceSink(grpByExprs, dest,
        reduceSinkInputRowResolver, reduceSinkOutputRowResolver, outputKeyColumnNames,
        colExprMap);

    int keyLength = reduceKeys.size();

    // add a key for reduce sink
    if (groupingSetsPresent) {
      // Process grouping set for the reduce sink operator
      processGroupingSetReduceSinkOperator(
          reduceSinkInputRowResolver,
          reduceSinkOutputRowResolver,
          reduceKeys,
          outputKeyColumnNames,
          colExprMap);

      if (changeNumPartitionFields) {
        numPartitionFields++;
      }
    }

    List<List<Integer>> distinctColIndices = getDistinctColIndicesForReduceSink(parseInfo, dest,
        reduceKeys, reduceSinkInputRowResolver, reduceSinkOutputRowResolver, outputKeyColumnNames,
        colExprMap);

    ArrayList<ExprNodeDesc> reduceValues = new ArrayList<ExprNodeDesc>();
    HashMap<String, ASTNode> aggregationTrees = parseInfo
        .getAggregationExprsForClause(dest);

    if (!mapAggrDone) {
      getReduceValuesForReduceSinkNoMapAgg(parseInfo, dest, reduceSinkInputRowResolver,
          reduceSinkOutputRowResolver, outputValueColumnNames, reduceValues, colExprMap);
    } else {
      // Put partial aggregation results in reduceValues
      int inputField = reduceKeys.size();

      for (Map.Entry<String, ASTNode> entry : aggregationTrees.entrySet()) {

        TypeInfo type = reduceSinkInputRowResolver.getColumnInfos().get(
            inputField).getType();
        ExprNodeColumnDesc exprDesc = new ExprNodeColumnDesc(type,
            getColumnInternalName(inputField), "", false);
        reduceValues.add(exprDesc);
        inputField++;
        String outputColName = getColumnInternalName(reduceValues.size() - 1);
        outputValueColumnNames.add(outputColName);
        String internalName = Utilities.ReduceField.VALUE.toString() + "."
            + outputColName;
        reduceSinkOutputRowResolver.putExpression(entry.getValue(),
            new ColumnInfo(internalName, type, null, false));
        colExprMap.put(internalName, exprDesc);
      }
    }

    ReduceSinkOperator rsOp = (ReduceSinkOperator) putOpInsertMap(
        OperatorFactory.getAndMakeChild(
            PlanUtils.getReduceSinkDesc(reduceKeys,
                groupingSetsPresent ? keyLength + 1 : keyLength,
                reduceValues, distinctColIndices,
                outputKeyColumnNames, outputValueColumnNames, true, -1, numPartitionFields,
                numReducers, AcidUtils.Operation.NOT_ACID),
            new RowSchema(reduceSinkOutputRowResolver.getColumnInfos()), inputOperatorInfo),
        reduceSinkOutputRowResolver);
    rsOp.setColumnExprMap(colExprMap);
    return rsOp;
  }

  private ArrayList<ExprNodeDesc> getReduceKeysForReduceSink(List<ASTNode> grpByExprs, String dest,
      RowResolver reduceSinkInputRowResolver, RowResolver reduceSinkOutputRowResolver,
      List<String> outputKeyColumnNames, Map<String, ExprNodeDesc> colExprMap)
      throws SemanticException {

    ArrayList<ExprNodeDesc> reduceKeys = new ArrayList<ExprNodeDesc>();

    for (int i = 0; i < grpByExprs.size(); ++i) {
      ASTNode grpbyExpr = grpByExprs.get(i);
      ExprNodeDesc inputExpr = genExprNodeDesc(grpbyExpr,
          reduceSinkInputRowResolver);
      ColumnInfo prev = reduceSinkOutputRowResolver.getExpression(grpbyExpr);
      if (prev != null && isDeterministic(inputExpr)) {
        colExprMap.put(prev.getInternalName(), inputExpr);
        continue;
      }
      reduceKeys.add(inputExpr);
      outputKeyColumnNames.add(getColumnInternalName(reduceKeys.size() - 1));
      String field = Utilities.ReduceField.KEY.toString() + "."
          + getColumnInternalName(reduceKeys.size() - 1);
      ColumnInfo colInfo = new ColumnInfo(field, reduceKeys.get(
          reduceKeys.size() - 1).getTypeInfo(), null, false);
      reduceSinkOutputRowResolver.putExpression(grpbyExpr, colInfo);
      colExprMap.put(colInfo.getInternalName(), inputExpr);
    }

    return reduceKeys;
  }

  private boolean isDeterministic(ExprNodeDesc expr) throws SemanticException {
    try {
      return ExprNodeEvaluatorFactory.get(expr).isDeterministic();
    } catch (Exception e) {
      throw new SemanticException(e);
    }
  }

  private List<List<Integer>> getDistinctColIndicesForReduceSink(QBParseInfo parseInfo,
      String dest,
      List<ExprNodeDesc> reduceKeys, RowResolver reduceSinkInputRowResolver,
      RowResolver reduceSinkOutputRowResolver, List<String> outputKeyColumnNames,
      Map<String, ExprNodeDesc> colExprMap)
      throws SemanticException {

    List<List<Integer>> distinctColIndices = new ArrayList<List<Integer>>();

    // If there is a distinctFuncExp, add all parameters to the reduceKeys.
    if (!parseInfo.getDistinctFuncExprsForClause(dest).isEmpty()) {
      List<ASTNode> distFuncs = parseInfo.getDistinctFuncExprsForClause(dest);
      String colName = getColumnInternalName(reduceKeys.size());
      outputKeyColumnNames.add(colName);
      for (int i = 0; i < distFuncs.size(); i++) {
        ASTNode value = distFuncs.get(i);
        int numExprs = 0;
        List<Integer> distinctIndices = new ArrayList<Integer>();
        // 0 is function name
        for (int j = 1; j < value.getChildCount(); j++) {
          ASTNode parameter = (ASTNode) value.getChild(j);
          ExprNodeDesc expr = genExprNodeDesc(parameter, reduceSinkInputRowResolver);
          // see if expr is already present in reduceKeys.
          // get index of expr in reduceKeys
          int ri;
          for (ri = 0; ri < reduceKeys.size(); ri++) {
            if (reduceKeys.get(ri).getExprString().equals(expr.getExprString())) {
              break;
            }
          }
          // add the expr to reduceKeys if it is not present
          if (ri == reduceKeys.size()) {
            String name = getColumnInternalName(numExprs);
            String field = Utilities.ReduceField.KEY.toString() + "." + colName
                + ":" + i
                + "." + name;
            ColumnInfo colInfo = new ColumnInfo(field, expr.getTypeInfo(), null, false);
            reduceSinkOutputRowResolver.putExpression(parameter, colInfo);
            colExprMap.put(field, expr);
            reduceKeys.add(expr);
          }
          // add the index of expr in reduceKeys to distinctIndices
          distinctIndices.add(ri);
          numExprs++;
        }
        distinctColIndices.add(distinctIndices);
      }
    }

    return distinctColIndices;
  }

  private void getReduceValuesForReduceSinkNoMapAgg(QBParseInfo parseInfo, String dest,
      RowResolver reduceSinkInputRowResolver, RowResolver reduceSinkOutputRowResolver,
      List<String> outputValueColumnNames, ArrayList<ExprNodeDesc> reduceValues,
      Map<String, ExprNodeDesc> colExprMap) throws SemanticException {
    HashMap<String, ASTNode> aggregationTrees = parseInfo
        .getAggregationExprsForClause(dest);

    // Put parameters to aggregations in reduceValues
    for (Map.Entry<String, ASTNode> entry : aggregationTrees.entrySet()) {
      ASTNode value = entry.getValue();
      // 0 is function name
      for (int i = 1; i < value.getChildCount(); i++) {
        ASTNode parameter = (ASTNode) value.getChild(i);
        if (reduceSinkOutputRowResolver.getExpression(parameter) == null) {
          ExprNodeDesc exprDesc = genExprNodeDesc(parameter, reduceSinkInputRowResolver);
          reduceValues.add(exprDesc);
          outputValueColumnNames
              .add(getColumnInternalName(reduceValues.size() - 1));
          String field = Utilities.ReduceField.VALUE.toString() + "."
              + getColumnInternalName(reduceValues.size() - 1);
          reduceSinkOutputRowResolver.putExpression(parameter, new ColumnInfo(field,
              reduceValues.get(reduceValues.size() - 1).getTypeInfo(), null,
              false));
          colExprMap.put(field, exprDesc);
        }
      }
    }
  }

  @SuppressWarnings("nls")
  private ReduceSinkOperator genCommonGroupByPlanReduceSinkOperator(QB qb, List<String> dests,
      Operator inputOperatorInfo) throws SemanticException {

    RowResolver reduceSinkInputRowResolver = opParseCtx.get(inputOperatorInfo)
        .getRowResolver();
    QBParseInfo parseInfo = qb.getParseInfo();
    RowResolver reduceSinkOutputRowResolver = new RowResolver();
    reduceSinkOutputRowResolver.setIsExprResolver(true);
    Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();

    // The group by keys and distinct keys should be the same for all dests, so using the first
    // one to produce these will be the same as using any other.
    String dest = dests.get(0);

    // Pre-compute group-by keys and store in reduceKeys
    List<String> outputKeyColumnNames = new ArrayList<String>();
    List<String> outputValueColumnNames = new ArrayList<String>();
    List<ASTNode> grpByExprs = getGroupByForClause(parseInfo, dest);

    ArrayList<ExprNodeDesc> reduceKeys = getReduceKeysForReduceSink(grpByExprs, dest,
        reduceSinkInputRowResolver, reduceSinkOutputRowResolver, outputKeyColumnNames,
        colExprMap);

    int keyLength = reduceKeys.size();

    List<List<Integer>> distinctColIndices = getDistinctColIndicesForReduceSink(parseInfo, dest,
        reduceKeys, reduceSinkInputRowResolver, reduceSinkOutputRowResolver, outputKeyColumnNames,
        colExprMap);

    ArrayList<ExprNodeDesc> reduceValues = new ArrayList<ExprNodeDesc>();

    // The dests can have different non-distinct aggregations, so we have to iterate over all of
    // them
    for (String destination : dests) {

      getReduceValuesForReduceSinkNoMapAgg(parseInfo, destination, reduceSinkInputRowResolver,
          reduceSinkOutputRowResolver, outputValueColumnNames, reduceValues, colExprMap);

      // Need to pass all of the columns used in the where clauses as reduce values
      ASTNode whereClause = parseInfo.getWhrForClause(destination);
      if (whereClause != null) {
        assert whereClause.getChildCount() == 1;
        ASTNode predicates = (ASTNode) whereClause.getChild(0);

        Map<ASTNode, ExprNodeDesc> nodeOutputs =
            genAllExprNodeDesc(predicates, reduceSinkInputRowResolver);
        removeMappingForKeys(predicates, nodeOutputs, reduceKeys);

        // extract columns missing in current RS key/value
        for (Map.Entry<ASTNode, ExprNodeDesc> entry : nodeOutputs.entrySet()) {
          ASTNode parameter = entry.getKey();
          ExprNodeDesc expression = entry.getValue();
          if (!(expression instanceof ExprNodeColumnDesc)) {
            continue;
          }
          if (ExprNodeDescUtils.indexOf(expression, reduceValues) >= 0) {
            continue;
          }
          String internalName = getColumnInternalName(reduceValues.size());
          String field = Utilities.ReduceField.VALUE.toString() + "." + internalName;

          reduceValues.add(expression);
          outputValueColumnNames.add(internalName);
          reduceSinkOutputRowResolver.putExpression(parameter,
              new ColumnInfo(field, expression.getTypeInfo(), null, false));
          colExprMap.put(field, expression);
        }
      }
    }

    // Optimize the scenario when there are no grouping keys - only 1 reducer is needed
    int numReducers = -1;
    if (grpByExprs.isEmpty()) {
      numReducers = 1;
    }
    ReduceSinkDesc rsDesc = PlanUtils.getReduceSinkDesc(reduceKeys, keyLength, reduceValues,
        distinctColIndices, outputKeyColumnNames, outputValueColumnNames,
        true, -1, keyLength, numReducers, AcidUtils.Operation.NOT_ACID);

    ReduceSinkOperator rsOp = (ReduceSinkOperator) putOpInsertMap(
        OperatorFactory.getAndMakeChild(rsDesc, new RowSchema(reduceSinkOutputRowResolver
            .getColumnInfos()), inputOperatorInfo), reduceSinkOutputRowResolver);
    rsOp.setColumnExprMap(colExprMap);
    return rsOp;
  }

  // Remove expression node descriptor and children of it for a given predicate
  // from mapping if it's already on RS keys.
  // Remaining column expressions would be a candidate for an RS value
  private void removeMappingForKeys(ASTNode predicate, Map<ASTNode, ExprNodeDesc> mapping,
      List<ExprNodeDesc> keys) {
    ExprNodeDesc expr = mapping.get(predicate);
    if (expr != null && ExprNodeDescUtils.indexOf(expr, keys) >= 0) {
      removeRecursively(predicate, mapping);
    } else {
      for (int i = 0; i < predicate.getChildCount(); i++) {
        removeMappingForKeys((ASTNode) predicate.getChild(i), mapping, keys);
      }
    }
  }

  // Remove expression node desc and all children of it from mapping
  private void removeRecursively(ASTNode current, Map<ASTNode, ExprNodeDesc> mapping) {
    mapping.remove(current);
    for (int i = 0; i < current.getChildCount(); i++) {
      removeRecursively((ASTNode) current.getChild(i), mapping);
    }
  }

  /**
   * Generate the second ReduceSinkOperator for the Group By Plan
   * (parseInfo.getXXX(dest)). The new ReduceSinkOperator will be a child of
   * groupByOperatorInfo.
   *
   * The second ReduceSinkOperator will put the group by keys in the map-reduce
   * sort key, and put the partial aggregation results in the map-reduce value.
   *
   * @param numPartitionFields
   *          the number of fields in the map-reduce partition key. This should
   *          always be the same as the number of Group By keys. We should be
   *          able to remove this parameter since in this phase there is no
   *          distinct any more.
   * @return the new ReduceSinkOperator.
   * @throws SemanticException
   */
  @SuppressWarnings("nls")
  private Operator genGroupByPlanReduceSinkOperator2MR(QBParseInfo parseInfo,
      String dest,
      Operator groupByOperatorInfo,
      int numPartitionFields,
      int numReducers,
      boolean groupingSetsPresent) throws SemanticException {
    RowResolver reduceSinkInputRowResolver2 = opParseCtx.get(
        groupByOperatorInfo).getRowResolver();
    RowResolver reduceSinkOutputRowResolver2 = new RowResolver();
    reduceSinkOutputRowResolver2.setIsExprResolver(true);
    Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();
    ArrayList<ExprNodeDesc> reduceKeys = new ArrayList<ExprNodeDesc>();
    ArrayList<String> outputColumnNames = new ArrayList<String>();
    // Get group-by keys and store in reduceKeys
    List<ASTNode> grpByExprs = getGroupByForClause(parseInfo, dest);
    for (int i = 0; i < grpByExprs.size(); ++i) {
      ASTNode grpbyExpr = grpByExprs.get(i);
      String field = getColumnInternalName(i);
      outputColumnNames.add(field);
      TypeInfo typeInfo = reduceSinkInputRowResolver2.getExpression(
          grpbyExpr).getType();
      ExprNodeColumnDesc inputExpr = new ExprNodeColumnDesc(typeInfo, field,
          "", false);
      reduceKeys.add(inputExpr);
      ColumnInfo colInfo = new ColumnInfo(Utilities.ReduceField.KEY.toString()
          + "." + field, typeInfo, "", false);
      reduceSinkOutputRowResolver2.putExpression(grpbyExpr, colInfo);
      colExprMap.put(colInfo.getInternalName(), inputExpr);
    }

    // add a key for reduce sink
    if (groupingSetsPresent) {
      // Note that partitioning fields dont need to change, since it is either
      // partitioned randomly, or by all grouping keys + distinct keys
      processGroupingSetReduceSinkOperator(
          reduceSinkInputRowResolver2,
          reduceSinkOutputRowResolver2,
          reduceKeys,
          outputColumnNames,
          colExprMap);
    }

    // Get partial aggregation results and store in reduceValues
    ArrayList<ExprNodeDesc> reduceValues = new ArrayList<ExprNodeDesc>();
    int inputField = reduceKeys.size();
    HashMap<String, ASTNode> aggregationTrees = parseInfo
        .getAggregationExprsForClause(dest);
    for (Map.Entry<String, ASTNode> entry : aggregationTrees.entrySet()) {
      String field = getColumnInternalName(inputField);
      ASTNode t = entry.getValue();
      TypeInfo typeInfo = reduceSinkInputRowResolver2.getExpression(t)
          .getType();
      ExprNodeColumnDesc exprDesc = new ExprNodeColumnDesc(typeInfo, field, "", false);
      reduceValues.add(exprDesc);
      inputField++;
      String col = getColumnInternalName(reduceValues.size() - 1);
      outputColumnNames.add(col);
      reduceSinkOutputRowResolver2.putExpression(t, new ColumnInfo(
          Utilities.ReduceField.VALUE.toString() + "." + col, typeInfo, "",
          false));
      colExprMap.put(col, exprDesc);
    }

    ReduceSinkOperator rsOp = (ReduceSinkOperator) putOpInsertMap(
        OperatorFactory.getAndMakeChild(PlanUtils.getReduceSinkDesc(reduceKeys,
            reduceValues, outputColumnNames, true, -1, numPartitionFields,
            numReducers, AcidUtils.Operation.NOT_ACID),
            new RowSchema(reduceSinkOutputRowResolver2.getColumnInfos()), groupByOperatorInfo),
        reduceSinkOutputRowResolver2);

    rsOp.setColumnExprMap(colExprMap);
    return rsOp;
  }

  /**
   * Generate the second GroupByOperator for the Group By Plan
   * (parseInfo.getXXX(dest)). The new GroupByOperator will do the second
   * aggregation based on the partial aggregation results.
   *
   * @param mode
   *          the mode of aggregation (FINAL)
   * @param genericUDAFEvaluators
   *          The mapping from Aggregation StringTree to the
   *          genericUDAFEvaluator.
   * @return the new GroupByOperator
   * @throws SemanticException
   */
  @SuppressWarnings("nls")
  private Operator genGroupByPlanGroupByOperator2MR(QBParseInfo parseInfo,
      String dest,
      Operator reduceSinkOperatorInfo2,
      GroupByDesc.Mode mode,
      Map<String, GenericUDAFEvaluator> genericUDAFEvaluators,
      boolean groupingSetsPresent) throws SemanticException {

    RowResolver groupByInputRowResolver2 = opParseCtx.get(
        reduceSinkOperatorInfo2).getRowResolver();
    RowResolver groupByOutputRowResolver2 = new RowResolver();
    groupByOutputRowResolver2.setIsExprResolver(true);
    ArrayList<ExprNodeDesc> groupByKeys = new ArrayList<ExprNodeDesc>();
    ArrayList<AggregationDesc> aggregations = new ArrayList<AggregationDesc>();
    Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();
    List<ASTNode> grpByExprs = getGroupByForClause(parseInfo, dest);
    ArrayList<String> outputColumnNames = new ArrayList<String>();
    for (int i = 0; i < grpByExprs.size(); ++i) {
      ASTNode grpbyExpr = grpByExprs.get(i);
      ColumnInfo exprInfo = groupByInputRowResolver2.getExpression(grpbyExpr);
      if (exprInfo == null) {
        throw new SemanticException(ErrorMsg.INVALID_COLUMN.getMsg(grpbyExpr));
      }

      String expression = exprInfo.getInternalName();
      groupByKeys.add(new ExprNodeColumnDesc(exprInfo.getType(), expression,
          exprInfo.getTabAlias(), exprInfo.getIsVirtualCol()));
      String field = getColumnInternalName(i);
      outputColumnNames.add(field);
      ColumnInfo oColInfo = new ColumnInfo(field, exprInfo.getType(), "", false);
      groupByOutputRowResolver2.putExpression(grpbyExpr,
          oColInfo);
      addAlternateGByKeyMappings(grpbyExpr, oColInfo, reduceSinkOperatorInfo2, groupByOutputRowResolver2);
      colExprMap.put(field, groupByKeys.get(groupByKeys.size() - 1));
    }

    int groupingSetsPosition = -1;
    // For grouping sets, add a dummy grouping key
    if (groupingSetsPresent) {
      groupingSetsPosition = groupByKeys.size();
      addGroupingSetKey(
          groupByKeys,
          groupByInputRowResolver2,
          groupByOutputRowResolver2,
          outputColumnNames,
          colExprMap);
    }

    HashMap<String, ASTNode> aggregationTrees = parseInfo
        .getAggregationExprsForClause(dest);
    boolean containsDistinctAggr = false;
    for (Map.Entry<String, ASTNode> entry : aggregationTrees.entrySet()) {
      ArrayList<ExprNodeDesc> aggParameters = new ArrayList<ExprNodeDesc>();
      ASTNode value = entry.getValue();
      ColumnInfo paraExprInfo = groupByInputRowResolver2.getExpression(value);
      if (paraExprInfo == null) {
        throw new SemanticException(ErrorMsg.INVALID_COLUMN.getMsg(value));
      }
      String paraExpression = paraExprInfo.getInternalName();
      assert (paraExpression != null);
      aggParameters.add(new ExprNodeColumnDesc(paraExprInfo.getType(),
          paraExpression, paraExprInfo.getTabAlias(), paraExprInfo
              .getIsVirtualCol()));

      String aggName = unescapeIdentifier(value.getChild(0).getText());

      boolean isDistinct = value.getType() == HiveParser.TOK_FUNCTIONDI;
      containsDistinctAggr = containsDistinctAggr || isDistinct;
      boolean isStar = value.getType() == HiveParser.TOK_FUNCTIONSTAR;
      Mode amode = groupByDescModeToUDAFMode(mode, isDistinct);
      GenericUDAFEvaluator genericUDAFEvaluator = genericUDAFEvaluators
          .get(entry.getKey());
      assert (genericUDAFEvaluator != null);
      GenericUDAFInfo udaf = getGenericUDAFInfo(genericUDAFEvaluator, amode,
          aggParameters);
      aggregations
          .add(new AggregationDesc(
              aggName.toLowerCase(),
              udaf.genericUDAFEvaluator,
              udaf.convertedParameters,
              (mode != GroupByDesc.Mode.FINAL && value.getToken().getType() ==
              HiveParser.TOK_FUNCTIONDI),
              amode));
      String field = getColumnInternalName(groupByKeys.size()
          + aggregations.size() - 1);
      outputColumnNames.add(field);
      groupByOutputRowResolver2.putExpression(value, new ColumnInfo(
          field, udaf.returnType, "", false));
    }
    float groupByMemoryUsage = HiveConf.getFloatVar(conf, HiveConf.ConfVars.HIVEMAPAGGRHASHMEMORY);
    float memoryThreshold = HiveConf
        .getFloatVar(conf, HiveConf.ConfVars.HIVEMAPAGGRMEMORYTHRESHOLD);

    Operator op = putOpInsertMap(OperatorFactory.getAndMakeChild(
        new GroupByDesc(mode, outputColumnNames, groupByKeys, aggregations,
            false, groupByMemoryUsage, memoryThreshold, null, false,
            groupingSetsPosition, containsDistinctAggr),
        new RowSchema(groupByOutputRowResolver2.getColumnInfos()),
        reduceSinkOperatorInfo2), groupByOutputRowResolver2);
    op.setColumnExprMap(colExprMap);
    return op;
  }

  /**
   * Generate a Group-By plan using a single map-reduce job (3 operators will be
   * inserted):
   *
   * ReduceSink ( keys = (K1_EXP, K2_EXP, DISTINCT_EXP), values = (A1_EXP,
   * A2_EXP) ) SortGroupBy (keys = (KEY.0,KEY.1), aggregations =
   * (count_distinct(KEY.2), sum(VALUE.0), count(VALUE.1))) Select (final
   * selects).
   *
   * @param dest
   * @param qb
   * @param input
   * @return
   * @throws SemanticException
   *
   *           Generate a Group-By plan using 1 map-reduce job. Spray by the
   *           group by key, and sort by the distinct key (if any), and compute
   *           aggregates * The aggregation evaluation functions are as
   *           follows: Partitioning Key: grouping key
   *
   *           Sorting Key: grouping key if no DISTINCT grouping + distinct key
   *           if DISTINCT
   *
   *           Reducer: iterate/merge (mode = COMPLETE)
   **/
  @SuppressWarnings({"nls"})
  private Operator genGroupByPlan1MR(String dest, QB qb, Operator input)
      throws SemanticException {

    QBParseInfo parseInfo = qb.getParseInfo();

    int numReducers = -1;
    ObjectPair<List<ASTNode>, List<Integer>> grpByExprsGroupingSets =
        getGroupByGroupingSetsForClause(parseInfo, dest);

    List<ASTNode> grpByExprs = grpByExprsGroupingSets.getFirst();
    List<Integer> groupingSets = grpByExprsGroupingSets.getSecond();

    if (grpByExprs.isEmpty()) {
      numReducers = 1;
    }

    // Grouping sets are not allowed
    if (!groupingSets.isEmpty()) {
      throw new SemanticException(ErrorMsg.HIVE_GROUPING_SETS_AGGR_NOMAPAGGR.getMsg());
    }

    // ////// 1. Generate ReduceSinkOperator
    ReduceSinkOperator reduceSinkOperatorInfo =
        genGroupByPlanReduceSinkOperator(qb,
            dest,
            input,
            grpByExprs,
            grpByExprs.size(),
            false,
            numReducers,
            false,
            false);

    // ////// 2. Generate GroupbyOperator
    Operator groupByOperatorInfo = genGroupByPlanGroupByOperator(parseInfo,
        dest, reduceSinkOperatorInfo, reduceSinkOperatorInfo, GroupByDesc.Mode.COMPLETE, null);

    return groupByOperatorInfo;
  }

  @SuppressWarnings({"nls"})
  private Operator genGroupByPlan1ReduceMultiGBY(List<String> dests, QB qb, Operator input,
      Map<String, Operator> aliasToOpInfo)
      throws SemanticException {

    QBParseInfo parseInfo = qb.getParseInfo();

    ExprNodeDesc previous = null;
    Operator selectInput = input;

    // In order to facilitate partition pruning, or the where clauses together and put them at the
    // top of the operator tree, this could also reduce the amount of data going to the reducer
    List<ExprNodeDesc.ExprNodeDescEqualityWrapper> whereExpressions =
        new ArrayList<ExprNodeDesc.ExprNodeDescEqualityWrapper>();
    for (String dest : dests) {
      ASTNode whereExpr = parseInfo.getWhrForClause(dest);

      if (whereExpr != null) {
        OpParseContext inputCtx = opParseCtx.get(input);
        RowResolver inputRR = inputCtx.getRowResolver();
        ExprNodeDesc current = genExprNodeDesc((ASTNode) whereExpr.getChild(0), inputRR);

        // Check the list of where expressions already added so they aren't duplicated
        ExprNodeDesc.ExprNodeDescEqualityWrapper currentWrapped =
            new ExprNodeDesc.ExprNodeDescEqualityWrapper(current);
        if (!whereExpressions.contains(currentWrapped)) {
          whereExpressions.add(currentWrapped);
        } else {
          continue;
        }

        if (previous == null) {
          // If this is the first expression
          previous = current;
          continue;
        }

        GenericUDFOPOr or = new GenericUDFOPOr();
        List<ExprNodeDesc> expressions = new ArrayList<ExprNodeDesc>(2);
        expressions.add(previous);
        expressions.add(current);
        ExprNodeDesc orExpr =
            new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo, or, expressions);
        previous = orExpr;
      } else {
        // If an expression does not have a where clause, there can be no common filter
        previous = null;
        break;
      }
    }

    if (previous != null) {
      OpParseContext inputCtx = opParseCtx.get(input);
      RowResolver inputRR = inputCtx.getRowResolver();
      FilterDesc orFilterDesc = new FilterDesc(previous, false);

      selectInput = putOpInsertMap(OperatorFactory.getAndMakeChild(
          orFilterDesc, new RowSchema(
              inputRR.getColumnInfos()), input), inputRR);
    }

    // insert a select operator here used by the ColumnPruner to reduce
    // the data to shuffle
    Operator select = genSelectAllDesc(selectInput);

    // Generate ReduceSinkOperator
    ReduceSinkOperator reduceSinkOperatorInfo =
        genCommonGroupByPlanReduceSinkOperator(qb, dests, select);

    // It is assumed throughout the code that a reducer has a single child, add a
    // ForwardOperator so that we can add multiple filter/group by operators as children
    RowResolver reduceSinkOperatorInfoRR = opParseCtx.get(reduceSinkOperatorInfo).getRowResolver();
    Operator forwardOp = putOpInsertMap(OperatorFactory.getAndMakeChild(new ForwardDesc(),
        new RowSchema(reduceSinkOperatorInfoRR.getColumnInfos()), reduceSinkOperatorInfo),
        reduceSinkOperatorInfoRR);

    Operator curr = forwardOp;

    for (String dest : dests) {
      curr = forwardOp;

      if (parseInfo.getWhrForClause(dest) != null) {
        ASTNode whereExpr = qb.getParseInfo().getWhrForClause(dest);
        curr = genFilterPlan((ASTNode) whereExpr.getChild(0), qb, forwardOp, aliasToOpInfo, false);
      }

      // Generate GroupbyOperator
      Operator groupByOperatorInfo = genGroupByPlanGroupByOperator(parseInfo,
          dest, curr, reduceSinkOperatorInfo, GroupByDesc.Mode.COMPLETE, null);

      // TODO: should we pass curr instead of null?
      curr = genPostGroupByBodyPlan(groupByOperatorInfo, dest, qb, aliasToOpInfo, null);
    }

    return curr;
  }

  static ArrayList<GenericUDAFEvaluator> getUDAFEvaluators(
      ArrayList<AggregationDesc> aggs) {
    ArrayList<GenericUDAFEvaluator> result = new ArrayList<GenericUDAFEvaluator>();
    for (int i = 0; i < aggs.size(); i++) {
      result.add(aggs.get(i).getGenericUDAFEvaluator());
    }
    return result;
  }

  /**
   * Generate a Multi Group-By plan using a 2 map-reduce jobs.
   *
   * @param dest
   * @param qb
   * @param input
   * @return
   * @throws SemanticException
   *
   *           Generate a Group-By plan using a 2 map-reduce jobs. Spray by the
   *           distinct key in hope of getting a uniform distribution, and
   *           compute partial aggregates by the grouping key. Evaluate partial
   *           aggregates first, and spray by the grouping key to compute actual
   *           aggregates in the second phase. The aggregation evaluation
   *           functions are as follows: Partitioning Key: distinct key
   *
   *           Sorting Key: distinct key
   *
   *           Reducer: iterate/terminatePartial (mode = PARTIAL1)
   *
   *           STAGE 2
   *
   *           Partitioning Key: grouping key
   *
   *           Sorting Key: grouping key
   *
   *           Reducer: merge/terminate (mode = FINAL)
   */
  @SuppressWarnings("nls")
  private Operator genGroupByPlan2MRMultiGroupBy(String dest, QB qb,
      Operator input) throws SemanticException {

    // ////// Generate GroupbyOperator for a map-side partial aggregation
    Map<String, GenericUDAFEvaluator> genericUDAFEvaluators =
        new LinkedHashMap<String, GenericUDAFEvaluator>();

    QBParseInfo parseInfo = qb.getParseInfo();

    // ////// 2. Generate GroupbyOperator
    Operator groupByOperatorInfo = genGroupByPlanGroupByOperator1(parseInfo,
        dest, input, GroupByDesc.Mode.HASH, genericUDAFEvaluators, true,
        null, false, false);

    int numReducers = -1;
    List<ASTNode> grpByExprs = getGroupByForClause(parseInfo, dest);

    // ////// 3. Generate ReduceSinkOperator2
    Operator reduceSinkOperatorInfo2 = genGroupByPlanReduceSinkOperator2MR(
        parseInfo, dest, groupByOperatorInfo, grpByExprs.size(), numReducers, false);

    // ////// 4. Generate GroupbyOperator2
    Operator groupByOperatorInfo2 = genGroupByPlanGroupByOperator2MR(parseInfo,
        dest, reduceSinkOperatorInfo2, GroupByDesc.Mode.FINAL,
        genericUDAFEvaluators, false);

    return groupByOperatorInfo2;
  }

  /**
   * Generate a Group-By plan using a 2 map-reduce jobs (5 operators will be
   * inserted):
   *
   * ReduceSink ( keys = (K1_EXP, K2_EXP, DISTINCT_EXP), values = (A1_EXP,
   * A2_EXP) ) NOTE: If DISTINCT_EXP is null, partition by rand() SortGroupBy
   * (keys = (KEY.0,KEY.1), aggregations = (count_distinct(KEY.2), sum(VALUE.0),
   * count(VALUE.1))) ReduceSink ( keys = (0,1), values=(2,3,4)) SortGroupBy
   * (keys = (KEY.0,KEY.1), aggregations = (sum(VALUE.0), sum(VALUE.1),
   * sum(VALUE.2))) Select (final selects).
   *
   * @param dest
   * @param qb
   * @param input
   * @return
   * @throws SemanticException
   *
   *           Generate a Group-By plan using a 2 map-reduce jobs. Spray by the
   *           grouping key and distinct key (or a random number, if no distinct
   *           is present) in hope of getting a uniform distribution, and
   *           compute partial aggregates grouped by the reduction key (grouping
   *           key + distinct key). Evaluate partial aggregates first, and spray
   *           by the grouping key to compute actual aggregates in the second
   *           phase. The aggregation evaluation functions are as follows:
   *           Partitioning Key: random() if no DISTINCT grouping + distinct key
   *           if DISTINCT
   *
   *           Sorting Key: grouping key if no DISTINCT grouping + distinct key
   *           if DISTINCT
   *
   *           Reducer: iterate/terminatePartial (mode = PARTIAL1)
   *
   *           STAGE 2
   *
   *           Partitioning Key: grouping key
   *
   *           Sorting Key: grouping key if no DISTINCT grouping + distinct key
   *           if DISTINCT
   *
   *           Reducer: merge/terminate (mode = FINAL)
   */
  @SuppressWarnings("nls")
  private Operator genGroupByPlan2MR(String dest, QB qb, Operator input)
      throws SemanticException {

    QBParseInfo parseInfo = qb.getParseInfo();

    ObjectPair<List<ASTNode>, List<Integer>> grpByExprsGroupingSets =
        getGroupByGroupingSetsForClause(parseInfo, dest);

    List<ASTNode> grpByExprs = grpByExprsGroupingSets.getFirst();
    List<Integer> groupingSets = grpByExprsGroupingSets.getSecond();

    // Grouping sets are not allowed
    // This restriction can be lifted in future.
    // HIVE-3508 has been filed for this
    if (!groupingSets.isEmpty()) {
      throw new SemanticException(ErrorMsg.HIVE_GROUPING_SETS_AGGR_NOMAPAGGR.getMsg());
    }

    // ////// 1. Generate ReduceSinkOperator
    // There is a special case when we want the rows to be randomly distributed
    // to
    // reducers for load balancing problem. That happens when there is no
    // DISTINCT
    // operator. We set the numPartitionColumns to -1 for this purpose. This is
    // captured by WritableComparableHiveObject.hashCode() function.
    ReduceSinkOperator reduceSinkOperatorInfo =
        genGroupByPlanReduceSinkOperator(qb,
            dest,
            input,
            grpByExprs,
            (parseInfo.getDistinctFuncExprsForClause(dest).isEmpty() ? -1 : Integer.MAX_VALUE),
            false,
            -1,
            false,
            false);

    // ////// 2. Generate GroupbyOperator
    Map<String, GenericUDAFEvaluator> genericUDAFEvaluators =
        new LinkedHashMap<String, GenericUDAFEvaluator>();
    GroupByOperator groupByOperatorInfo = (GroupByOperator) genGroupByPlanGroupByOperator(
        parseInfo, dest, reduceSinkOperatorInfo, reduceSinkOperatorInfo, GroupByDesc.Mode.PARTIAL1,
        genericUDAFEvaluators);

    int numReducers = -1;
    if (grpByExprs.isEmpty()) {
      numReducers = 1;
    }

    // ////// 3. Generate ReduceSinkOperator2
    Operator reduceSinkOperatorInfo2 = genGroupByPlanReduceSinkOperator2MR(
        parseInfo, dest, groupByOperatorInfo, grpByExprs.size(), numReducers, false);

    // ////// 4. Generate GroupbyOperator2
    Operator groupByOperatorInfo2 = genGroupByPlanGroupByOperator2MR(parseInfo,
        dest, reduceSinkOperatorInfo2, GroupByDesc.Mode.FINAL,
        genericUDAFEvaluators, false);

    return groupByOperatorInfo2;
  }

  private boolean optimizeMapAggrGroupBy(String dest, QB qb) {
    List<ASTNode> grpByExprs = getGroupByForClause(qb.getParseInfo(), dest);
    if ((grpByExprs != null) && !grpByExprs.isEmpty()) {
      return false;
    }

    if (!qb.getParseInfo().getDistinctFuncExprsForClause(dest).isEmpty()) {
      return false;
    }

    return true;
  }

  static private void extractColumns(Set<String> colNamesExprs,
      ExprNodeDesc exprNode) throws SemanticException {
    if (exprNode instanceof ExprNodeColumnDesc) {
      colNamesExprs.add(((ExprNodeColumnDesc) exprNode).getColumn());
      return;
    }

    if (exprNode instanceof ExprNodeGenericFuncDesc) {
      ExprNodeGenericFuncDesc funcDesc = (ExprNodeGenericFuncDesc) exprNode;
      for (ExprNodeDesc childExpr : funcDesc.getChildren()) {
        extractColumns(colNamesExprs, childExpr);
      }
    }
  }

  static private boolean hasCommonElement(Set<String> set1, Set<String> set2) {
    for (String elem1 : set1) {
      if (set2.contains(elem1)) {
        return true;
      }
    }

    return false;
  }

  private void checkExpressionsForGroupingSet(List<ASTNode> grpByExprs,
      List<ASTNode> distinctGrpByExprs,
      Map<String, ASTNode> aggregationTrees,
      RowResolver inputRowResolver) throws SemanticException {

    Set<String> colNamesGroupByExprs = new HashSet<String>();
    Set<String> colNamesGroupByDistinctExprs = new HashSet<String>();
    Set<String> colNamesAggregateParameters = new HashSet<String>();

    // The columns in the group by expressions should not intersect with the columns in the
    // distinct expressions
    for (ASTNode grpByExpr : grpByExprs) {
      extractColumns(colNamesGroupByExprs, genExprNodeDesc(grpByExpr, inputRowResolver));
    }

    // If there is a distinctFuncExp, add all parameters to the reduceKeys.
    if (!distinctGrpByExprs.isEmpty()) {
      for (ASTNode value : distinctGrpByExprs) {
        // 0 is function name
        for (int i = 1; i < value.getChildCount(); i++) {
          ASTNode parameter = (ASTNode) value.getChild(i);
          ExprNodeDesc distExprNode = genExprNodeDesc(parameter, inputRowResolver);
          // extract all the columns
          extractColumns(colNamesGroupByDistinctExprs, distExprNode);
        }

        if (hasCommonElement(colNamesGroupByExprs, colNamesGroupByDistinctExprs)) {
          throw new SemanticException(ErrorMsg.HIVE_GROUPING_SETS_AGGR_EXPRESSION_INVALID.getMsg());
        }
      }
    }

    for (Map.Entry<String, ASTNode> entry : aggregationTrees.entrySet()) {
      ASTNode value = entry.getValue();
      ArrayList<ExprNodeDesc> aggParameters = new ArrayList<ExprNodeDesc>();
      // 0 is the function name
      for (int i = 1; i < value.getChildCount(); i++) {
        ASTNode paraExpr = (ASTNode) value.getChild(i);
        ExprNodeDesc paraExprNode = genExprNodeDesc(paraExpr, inputRowResolver);

        // extract all the columns
        extractColumns(colNamesAggregateParameters, paraExprNode);
      }

      if (hasCommonElement(colNamesGroupByExprs, colNamesAggregateParameters)) {
        throw new SemanticException(ErrorMsg.HIVE_GROUPING_SETS_AGGR_EXPRESSION_INVALID.getMsg());
      }
    }
  }

  /**
   * Generate a Group-By plan using 1 map-reduce job. First perform a map-side
   * partial aggregation (to reduce the amount of data), at this point of time,
   * we may turn off map-side partial aggregation based on its performance. Then
   * spray by the group by key, and sort by the distinct key (if any), and
   * compute aggregates based on actual aggregates
   *
   * The aggregation evaluation functions are as follows:
   *
   * No grouping sets:
   * Group By Operator:
   * grouping keys: group by expressions if no DISTINCT
   * grouping keys: group by expressions + distinct keys if DISTINCT
   * Mapper: iterate/terminatePartial (mode = HASH)
   * Partitioning Key: grouping key
   * Sorting Key: grouping key if no DISTINCT
   * grouping + distinct key if DISTINCT
   * Reducer: iterate/terminate if DISTINCT
   * merge/terminate if NO DISTINCT (mode MERGEPARTIAL)
   *
   * Grouping Sets:
   * Group By Operator:
   * grouping keys: group by expressions + grouping id. if no DISTINCT
   * grouping keys: group by expressions + grouping id. + distinct keys if DISTINCT
   * Mapper: iterate/terminatePartial (mode = HASH)
   * Partitioning Key: grouping key + grouping id.
   * Sorting Key: grouping key + grouping id. if no DISTINCT
   * grouping + grouping id. + distinct key if DISTINCT
   * Reducer: iterate/terminate if DISTINCT
   * merge/terminate if NO DISTINCT (mode MERGEPARTIAL)
   *
   * Grouping Sets with an additional MR job introduced (distincts are not allowed):
   * Group By Operator:
   * grouping keys: group by expressions
   * Mapper: iterate/terminatePartial (mode = HASH)
   * Partitioning Key: grouping key
   * Sorting Key: grouping key
   * Reducer: merge/terminate (mode MERGEPARTIAL)
   * Group by Operator:
   * grouping keys: group by expressions + add a new grouping id. key
   *
   * STAGE 2
   * Partitioning Key: grouping key + grouping id.
   * Sorting Key: grouping key + grouping id.
   * Reducer: merge/terminate (mode = FINAL)
   * Group by Operator:
   * grouping keys: group by expressions + grouping id.
   */
  @SuppressWarnings("nls")
  private Operator genGroupByPlanMapAggrNoSkew(String dest, QB qb,
      Operator inputOperatorInfo) throws SemanticException {

    QBParseInfo parseInfo = qb.getParseInfo();
    ObjectPair<List<ASTNode>, List<Integer>> grpByExprsGroupingSets =
        getGroupByGroupingSetsForClause(parseInfo, dest);

    List<ASTNode> grpByExprs = grpByExprsGroupingSets.getFirst();
    List<Integer> groupingSets = grpByExprsGroupingSets.getSecond();
    boolean groupingSetsPresent = !groupingSets.isEmpty();

    int newMRJobGroupingSetsThreshold =
        conf.getIntVar(HiveConf.ConfVars.HIVE_NEW_JOB_GROUPING_SET_CARDINALITY);

    if (groupingSetsPresent) {
      checkExpressionsForGroupingSet(grpByExprs,
          parseInfo.getDistinctFuncExprsForClause(dest),
          parseInfo.getAggregationExprsForClause(dest),
          opParseCtx.get(inputOperatorInfo).getRowResolver());
    }

    // ////// Generate GroupbyOperator for a map-side partial aggregation
    Map<String, GenericUDAFEvaluator> genericUDAFEvaluators =
        new LinkedHashMap<String, GenericUDAFEvaluator>();

    // Is the grouping sets data consumed in the current in MR job, or
    // does it need an additional MR job
    boolean groupingSetsNeedAdditionalMRJob =
        groupingSetsPresent && groupingSets.size() > newMRJobGroupingSetsThreshold ?
            true : false;

    GroupByOperator groupByOperatorInfo =
        (GroupByOperator) genGroupByPlanMapGroupByOperator(
            qb,
            dest,
            grpByExprs,
            inputOperatorInfo,
            GroupByDesc.Mode.HASH,
            genericUDAFEvaluators,
            groupingSets,
            groupingSetsPresent && !groupingSetsNeedAdditionalMRJob);

    groupOpToInputTables.put(groupByOperatorInfo, opParseCtx.get(
        inputOperatorInfo).getRowResolver().getTableNames());
    int numReducers = -1;

    // Optimize the scenario when there are no grouping keys - only 1 reducer is
    // needed
    if (grpByExprs.isEmpty()) {
      numReducers = 1;
    }

    // ////// Generate ReduceSink Operator
    boolean isDistinct = !qb.getParseInfo().getDistinctFuncExprsForClause(dest).isEmpty();

    // Distincts are not allowed with an additional mr job
    if (groupingSetsNeedAdditionalMRJob && isDistinct) {
      String errorMsg = "The number of rows per input row due to grouping sets is "
          + groupingSets.size();
      throw new SemanticException(
          ErrorMsg.HIVE_GROUPING_SETS_THRESHOLD_NOT_ALLOWED_WITH_DISTINCTS.getMsg(errorMsg));
    }

    Operator reduceSinkOperatorInfo =
        genGroupByPlanReduceSinkOperator(qb,
            dest,
            groupByOperatorInfo,
            grpByExprs,
            grpByExprs.size(),
            true,
            numReducers,
            true,
            groupingSetsPresent && !groupingSetsNeedAdditionalMRJob);

    // Does it require a new MR job for grouping sets
    if (!groupingSetsPresent || !groupingSetsNeedAdditionalMRJob) {
      // This is a 1-stage map-reduce processing of the groupby. Tha map-side
      // aggregates was just used to
      // reduce output data. In case of distincts, partial results are not used,
      // and so iterate is again
      // invoked on the reducer. In case of non-distincts, partial results are
      // used, and merge is invoked
      // on the reducer.
      return genGroupByPlanGroupByOperator1(parseInfo, dest,
          reduceSinkOperatorInfo, GroupByDesc.Mode.MERGEPARTIAL,
          genericUDAFEvaluators, false,
          groupingSets, groupingSetsPresent, groupingSetsNeedAdditionalMRJob);
    }
    else
    {
      // Add 'n' rows corresponding to the grouping sets. For each row, create 'n' rows,
      // one for each grouping set key. Since map-side aggregation has already been performed,
      // the number of rows would have been reduced. Moreover, the rows corresponding to the
      // grouping keys come together, so there is a higher chance of finding the rows in the hash
      // table.
      Operator groupByOperatorInfo2 =
          genGroupByPlanGroupByOperator1(parseInfo, dest,
              reduceSinkOperatorInfo, GroupByDesc.Mode.PARTIALS,
              genericUDAFEvaluators, false,
              groupingSets, groupingSetsPresent, groupingSetsNeedAdditionalMRJob);

      // ////// Generate ReduceSinkOperator2
      Operator reduceSinkOperatorInfo2 = genGroupByPlanReduceSinkOperator2MR(
          parseInfo, dest, groupByOperatorInfo2, grpByExprs.size() + 1, numReducers,
          groupingSetsPresent);

      // ////// Generate GroupbyOperator3
      return genGroupByPlanGroupByOperator2MR(parseInfo, dest,
          reduceSinkOperatorInfo2, GroupByDesc.Mode.FINAL,
          genericUDAFEvaluators, groupingSetsPresent);
    }
  }

  /**
   * Generate a Group-By plan using a 2 map-reduce jobs. However, only 1
   * group-by plan is generated if the query involves no grouping key and no
   * distincts. In that case, the plan is same as generated by
   * genGroupByPlanMapAggr1MR. Otherwise, the following plan is generated: First
   * perform a map side partial aggregation (to reduce the amount of data). Then
   * spray by the grouping key and distinct key (or a random number, if no
   * distinct is present) in hope of getting a uniform distribution, and compute
   * partial aggregates grouped by the reduction key (grouping key + distinct
   * key). Evaluate partial aggregates first, and spray by the grouping key to
   * compute actual aggregates in the second phase.
   *
   * The aggregation evaluation functions are as follows:
   *
   * No grouping sets:
   * STAGE 1
   * Group by Operator:
   * grouping keys: group by expressions if no DISTINCT
   * grouping keys: group by expressions + distinct keys if DISTINCT
   * Mapper: iterate/terminatePartial (mode = HASH)
   * Partitioning Key: random() if no DISTINCT
   * grouping + distinct key if DISTINCT
   * Sorting Key: grouping key if no DISTINCT
   * grouping + distinct key if DISTINCT
   * Reducer: iterate/terminatePartial if DISTINCT
   * merge/terminatePartial if NO DISTINCT (mode = MERGEPARTIAL)
   * Group by Operator:
   * grouping keys: group by expressions
   *
   * STAGE 2
   * Partitioning Key: grouping key
   * Sorting Key: grouping key
   * Reducer: merge/terminate (mode = FINAL)
   *
   * In the presence of grouping sets, the aggregation evaluation functions are as follows:
   * STAGE 1
   * Group by Operator:
   * grouping keys: group by expressions + grouping id. if no DISTINCT
   * grouping keys: group by expressions + + grouping id. + distinct keys if DISTINCT
   * Mapper: iterate/terminatePartial (mode = HASH)
   * Partitioning Key: random() if no DISTINCT
   * grouping + grouping id. + distinct key if DISTINCT
   * Sorting Key: grouping key + grouping id. if no DISTINCT
   * grouping + grouping id. + distinct key if DISTINCT
   * Reducer: iterate/terminatePartial if DISTINCT
   * merge/terminatePartial if NO DISTINCT (mode = MERGEPARTIAL)
   * Group by Operator:
   * grouping keys: group by expressions + grouping id.
   *
   * STAGE 2
   * Partitioning Key: grouping key
   * Sorting Key: grouping key + grouping id.
   * Reducer: merge/terminate (mode = FINAL)
   */
  @SuppressWarnings("nls")
  private Operator genGroupByPlanMapAggr2MR(String dest, QB qb,
      Operator inputOperatorInfo) throws SemanticException {

    QBParseInfo parseInfo = qb.getParseInfo();

    ObjectPair<List<ASTNode>, List<Integer>> grpByExprsGroupingSets =
        getGroupByGroupingSetsForClause(parseInfo, dest);

    List<ASTNode> grpByExprs = grpByExprsGroupingSets.getFirst();
    List<Integer> groupingSets = grpByExprsGroupingSets.getSecond();
    boolean groupingSetsPresent = !groupingSets.isEmpty();

    if (groupingSetsPresent) {
      checkExpressionsForGroupingSet(grpByExprs,
          parseInfo.getDistinctFuncExprsForClause(dest),
          parseInfo.getAggregationExprsForClause(dest),
          opParseCtx.get(inputOperatorInfo).getRowResolver());

      int newMRJobGroupingSetsThreshold =
          conf.getIntVar(HiveConf.ConfVars.HIVE_NEW_JOB_GROUPING_SET_CARDINALITY);

      // Turn off skew if an additional MR job is required anyway for grouping sets.
      if (groupingSets.size() > newMRJobGroupingSetsThreshold) {
        String errorMsg = "The number of rows per input row due to grouping sets is "
            + groupingSets.size();
        throw new SemanticException(
            ErrorMsg.HIVE_GROUPING_SETS_THRESHOLD_NOT_ALLOWED_WITH_SKEW.getMsg(errorMsg));
      }
    }

    // ////// Generate GroupbyOperator for a map-side partial aggregation
    Map<String, GenericUDAFEvaluator> genericUDAFEvaluators =
        new LinkedHashMap<String, GenericUDAFEvaluator>();
    GroupByOperator groupByOperatorInfo =
        (GroupByOperator) genGroupByPlanMapGroupByOperator(
            qb, dest, grpByExprs, inputOperatorInfo, GroupByDesc.Mode.HASH,
            genericUDAFEvaluators, groupingSets, groupingSetsPresent);

    groupOpToInputTables.put(groupByOperatorInfo, opParseCtx.get(
        inputOperatorInfo).getRowResolver().getTableNames());
    // Optimize the scenario when there are no grouping keys and no distinct - 2
    // map-reduce jobs are not needed
    // For eg: select count(1) from T where t.ds = ....
    if (!optimizeMapAggrGroupBy(dest, qb)) {
      List<ASTNode> distinctFuncExprs = parseInfo.getDistinctFuncExprsForClause(dest);

      // ////// Generate ReduceSink Operator
      Operator reduceSinkOperatorInfo =
          genGroupByPlanReduceSinkOperator(qb,
              dest,
              groupByOperatorInfo,
              grpByExprs,
              distinctFuncExprs.isEmpty() ? -1 : Integer.MAX_VALUE,
              false,
              -1,
              true,
              groupingSetsPresent);

      // ////// Generate GroupbyOperator for a partial aggregation
      Operator groupByOperatorInfo2 = genGroupByPlanGroupByOperator1(parseInfo,
          dest, reduceSinkOperatorInfo, GroupByDesc.Mode.PARTIALS,
          genericUDAFEvaluators, false,
          groupingSets, groupingSetsPresent, false);

      int numReducers = -1;
      if (grpByExprs.isEmpty()) {
        numReducers = 1;
      }

      // ////// Generate ReduceSinkOperator2
      Operator reduceSinkOperatorInfo2 = genGroupByPlanReduceSinkOperator2MR(
          parseInfo, dest, groupByOperatorInfo2, grpByExprs.size(), numReducers,
          groupingSetsPresent);

      // ////// Generate GroupbyOperator3
      return genGroupByPlanGroupByOperator2MR(parseInfo, dest,
          reduceSinkOperatorInfo2, GroupByDesc.Mode.FINAL,
          genericUDAFEvaluators, groupingSetsPresent);
    } else {
      // If there are no grouping keys, grouping sets cannot be present
      assert !groupingSetsPresent;

      // ////// Generate ReduceSink Operator
      Operator reduceSinkOperatorInfo =
          genGroupByPlanReduceSinkOperator(qb,
              dest,
              groupByOperatorInfo,
              grpByExprs,
              grpByExprs.size(),
              false,
              1,
              true,
              groupingSetsPresent);

      return genGroupByPlanGroupByOperator2MR(parseInfo, dest,
          reduceSinkOperatorInfo, GroupByDesc.Mode.FINAL, genericUDAFEvaluators, false);
    }
  }

  private int getReducersBucketing(int totalFiles, int maxReducers) {
    int numFiles = (int)Math.ceil((double)totalFiles / (double)maxReducers);
    while (true) {
      if (totalFiles % numFiles == 0) {
        return totalFiles / numFiles;
      }
      numFiles++;
    }
  }

  private static class SortBucketRSCtx {
    ArrayList<ExprNodeDesc> partnCols;
    boolean multiFileSpray;
    int numFiles;
    int totalFiles;

    public SortBucketRSCtx() {
      partnCols = null;
      multiFileSpray = false;
      numFiles = 1;
      totalFiles = 1;
    }

    /**
     * @return the partnCols
     */
    public ArrayList<ExprNodeDesc> getPartnCols() {
      return partnCols;
    }

    /**
     * @param partnCols
     *          the partnCols to set
     */
    public void setPartnCols(ArrayList<ExprNodeDesc> partnCols) {
      this.partnCols = partnCols;
    }

    /**
     * @return the multiFileSpray
     */
    public boolean isMultiFileSpray() {
      return multiFileSpray;
    }

    /**
     * @param multiFileSpray
     *          the multiFileSpray to set
     */
    public void setMultiFileSpray(boolean multiFileSpray) {
      this.multiFileSpray = multiFileSpray;
    }

    /**
     * @return the numFiles
     */
    public int getNumFiles() {
      return numFiles;
    }

    /**
     * @param numFiles
     *          the numFiles to set
     */
    public void setNumFiles(int numFiles) {
      this.numFiles = numFiles;
    }

    /**
     * @return the totalFiles
     */
    public int getTotalFiles() {
      return totalFiles;
    }

    /**
     * @param totalFiles
     *          the totalFiles to set
     */
    public void setTotalFiles(int totalFiles) {
      this.totalFiles = totalFiles;
    }
  }

  @SuppressWarnings("nls")
  private Operator genBucketingSortingDest(String dest, Operator input, QB qb,
      TableDesc table_desc, Table dest_tab, SortBucketRSCtx ctx) throws SemanticException {

    // If the table is bucketed, and bucketing is enforced, do the following:
    // If the number of buckets is smaller than the number of maximum reducers,
    // create those many reducers.
    // If not, create a multiFileSink instead of FileSink - the multiFileSink will
    // spray the data into multiple buckets. That way, we can support a very large
    // number of buckets without needing a very large number of reducers.
    boolean enforceBucketing = false;
    boolean enforceSorting = false;
    ArrayList<ExprNodeDesc> partnCols = new ArrayList<ExprNodeDesc>();
    ArrayList<ExprNodeDesc> partnColsNoConvert = new ArrayList<ExprNodeDesc>();
    ArrayList<ExprNodeDesc> sortCols = new ArrayList<ExprNodeDesc>();
    ArrayList<Integer> sortOrders = new ArrayList<Integer>();
    boolean multiFileSpray = false;
    int numFiles = 1;
    int totalFiles = 1;

    if ((dest_tab.getNumBuckets() > 0) &&
        (conf.getBoolVar(HiveConf.ConfVars.HIVEENFORCEBUCKETING))) {
      enforceBucketing = true;
      if (updating() || deleting()) {
        partnCols = getPartitionColsFromBucketColsForUpdateDelete(input, true);
        partnColsNoConvert = getPartitionColsFromBucketColsForUpdateDelete(input, false);
      } else {
        partnCols = getPartitionColsFromBucketCols(dest, qb, dest_tab, table_desc, input, true);
        partnColsNoConvert = getPartitionColsFromBucketCols(dest, qb, dest_tab, table_desc, input,
            false);
      }
    }

    if ((dest_tab.getSortCols() != null) &&
        (dest_tab.getSortCols().size() > 0) &&
        (conf.getBoolVar(HiveConf.ConfVars.HIVEENFORCESORTING))) {
      enforceSorting = true;
      sortCols = getSortCols(dest, qb, dest_tab, table_desc, input, true);
      sortOrders = getSortOrders(dest, qb, dest_tab, input);
      if (!enforceBucketing) {
        partnCols = sortCols;
        partnColsNoConvert = getSortCols(dest, qb, dest_tab, table_desc, input, false);
      }
    }

    if (enforceBucketing || enforceSorting) {
      int maxReducers = conf.getIntVar(HiveConf.ConfVars.MAXREDUCERS);
      if (conf.getIntVar(HiveConf.ConfVars.HADOOPNUMREDUCERS) > 0) {
        maxReducers = conf.getIntVar(HiveConf.ConfVars.HADOOPNUMREDUCERS);
      }
      int numBuckets = dest_tab.getNumBuckets();
      if (numBuckets > maxReducers) {
        LOG.debug("XXXXXX numBuckets is " + numBuckets + " and maxReducers is " + maxReducers);
        multiFileSpray = true;
        totalFiles = numBuckets;
        if (totalFiles % maxReducers == 0) {
          numFiles = totalFiles / maxReducers;
        }
        else {
          // find the number of reducers such that it is a divisor of totalFiles
          maxReducers = getReducersBucketing(totalFiles, maxReducers);
          numFiles = totalFiles / maxReducers;
        }
      }
      else {
        maxReducers = numBuckets;
      }

      input = genReduceSinkPlanForSortingBucketing(dest_tab, input,
          sortCols, sortOrders, partnCols, maxReducers);
      ctx.setMultiFileSpray(multiFileSpray);
      ctx.setNumFiles(numFiles);
      ctx.setPartnCols(partnColsNoConvert);
      ctx.setTotalFiles(totalFiles);
    }
    return input;
  }

  /**
   * Check for HOLD_DDLTIME hint.
   *
   * @param qb
   * @return true if HOLD_DDLTIME is set, false otherwise.
   */
  private boolean checkHoldDDLTime(QB qb) {
    ASTNode hints = qb.getParseInfo().getHints();
    if (hints == null) {
      return false;
    }
    for (int pos = 0; pos < hints.getChildCount(); pos++) {
      ASTNode hint = (ASTNode) hints.getChild(pos);
      if (((ASTNode) hint.getChild(0)).getToken().getType() == HiveParser.TOK_HOLD_DDLTIME) {
        return true;
      }
    }
    return false;
  }

  @SuppressWarnings("nls")
  private Operator genFileSinkPlan(String dest, QB qb, Operator input)
      throws SemanticException {

    RowResolver inputRR = opParseCtx.get(input).getRowResolver();
    QBMetaData qbm = qb.getMetaData();
    Integer dest_type = qbm.getDestTypeForAlias(dest);

    Table dest_tab = null; // destination table if any
    boolean destTableIsAcid = false; // should the destination table be written to using ACID
    Partition dest_part = null;// destination partition if any
    Path queryTmpdir = null; // the intermediate destination directory
    Path dest_path = null; // the final destination directory
    TableDesc table_desc = null;
    int currentTableId = 0;
    boolean isLocal = false;
    SortBucketRSCtx rsCtx = new SortBucketRSCtx();
    DynamicPartitionCtx dpCtx = null;
    LoadTableDesc ltd = null;
    boolean holdDDLTime = checkHoldDDLTime(qb);
    ListBucketingCtx lbCtx = null;

    switch (dest_type.intValue()) {
    case QBMetaData.DEST_TABLE: {

      dest_tab = qbm.getDestTableForAlias(dest);
      destTableIsAcid = isAcidTable(dest_tab);

      // Is the user trying to insert into a external tables
      if ((!conf.getBoolVar(HiveConf.ConfVars.HIVE_INSERT_INTO_EXTERNAL_TABLES)) &&
          (dest_tab.getTableType().equals(TableType.EXTERNAL_TABLE))) {
        throw new SemanticException(
            ErrorMsg.INSERT_EXTERNAL_TABLE.getMsg(dest_tab.getTableName()));
      }

      Map<String, String> partSpec = qbm.getPartSpecForAlias(dest);
      dest_path = dest_tab.getPath();

      // If the query here is an INSERT_INTO and the target is an immutable table,
      // verify that our destination is empty before proceeding
      if (dest_tab.isImmutable() &&
          qb.getParseInfo().isInsertIntoTable(dest_tab.getDbName(),dest_tab.getTableName())){
        try {
          FileSystem fs = dest_path.getFileSystem(conf);
          if (! MetaStoreUtils.isDirEmpty(fs,dest_path)){
            LOG.warn("Attempted write into an immutable table : "
                + dest_tab.getTableName() + " : " + dest_path);
            throw new SemanticException(
                ErrorMsg.INSERT_INTO_IMMUTABLE_TABLE.getMsg(dest_tab.getTableName()));
          }
        } catch (IOException ioe) {
            LOG.warn("Error while trying to determine if immutable table has any data : "
                + dest_tab.getTableName() + " : " + dest_path);
          throw new SemanticException(ErrorMsg.INSERT_INTO_IMMUTABLE_TABLE.getMsg(ioe.getMessage()));
        }
      }

      // check for partition
      List<FieldSchema> parts = dest_tab.getPartitionKeys();
      if (parts != null && parts.size() > 0) { // table is partitioned
        if (partSpec == null || partSpec.size() == 0) { // user did NOT specify partition
          throw new SemanticException(generateErrorMessage(
              qb.getParseInfo().getDestForClause(dest),
              ErrorMsg.NEED_PARTITION_ERROR.getMsg()));
        }
        // the HOLD_DDLTIIME hint should not be used with dynamic partition since the
        // newly generated partitions should always update their DDLTIME
        if (holdDDLTime) {
          throw new SemanticException(generateErrorMessage(
              qb.getParseInfo().getDestForClause(dest),
              ErrorMsg.HOLD_DDLTIME_ON_NONEXIST_PARTITIONS.getMsg()));
        }
        dpCtx = qbm.getDPCtx(dest);
        if (dpCtx == null) {
          dest_tab.validatePartColumnNames(partSpec, false);
          dpCtx = new DynamicPartitionCtx(dest_tab, partSpec,
              conf.getVar(HiveConf.ConfVars.DEFAULTPARTITIONNAME),
              conf.getIntVar(HiveConf.ConfVars.DYNAMICPARTITIONMAXPARTSPERNODE));
          qbm.setDPCtx(dest, dpCtx);
        }

        if (!HiveConf.getBoolVar(conf, HiveConf.ConfVars.DYNAMICPARTITIONING)) { // allow DP
          throw new SemanticException(generateErrorMessage(
              qb.getParseInfo().getDestForClause(dest),
              ErrorMsg.DYNAMIC_PARTITION_DISABLED.getMsg()));
        }
        if (dpCtx.getSPPath() != null) {
          dest_path = new Path(dest_tab.getPath(), dpCtx.getSPPath());
        }
        if ((dest_tab.getNumBuckets() > 0) &&
            (conf.getBoolVar(HiveConf.ConfVars.HIVEENFORCEBUCKETING))) {
          dpCtx.setNumBuckets(dest_tab.getNumBuckets());
        }
      }

      boolean isNonNativeTable = dest_tab.isNonNative();
      if (isNonNativeTable) {
        queryTmpdir = dest_path;
      } else {
        queryTmpdir = ctx.getExtTmpPathRelTo(dest_path);
      }
      if (dpCtx != null) {
        // set the root of the temporary path where dynamic partition columns will populate
        dpCtx.setRootPath(queryTmpdir);
      }
      // this table_desc does not contain the partitioning columns
      table_desc = Utilities.getTableDesc(dest_tab);

      // Add sorting/bucketing if needed
      input = genBucketingSortingDest(dest, input, qb, table_desc, dest_tab, rsCtx);

      idToTableNameMap.put(String.valueOf(destTableId), dest_tab.getTableName());
      currentTableId = destTableId;
      destTableId++;

      lbCtx = constructListBucketingCtx(dest_tab.getSkewedColNames(),
          dest_tab.getSkewedColValues(), dest_tab.getSkewedColValueLocationMaps(),
          dest_tab.isStoredAsSubDirectories(), conf);

      // Create the work for moving the table
      // NOTE: specify Dynamic partitions in dest_tab for WriteEntity
      if (!isNonNativeTable) {
        AcidUtils.Operation acidOp = AcidUtils.Operation.NOT_ACID;
        if (destTableIsAcid) {
          acidOp = getAcidType(table_desc.getOutputFileFormatClass());
          checkAcidConstraints(qb, table_desc, dest_tab);
        }
        ltd = new LoadTableDesc(queryTmpdir,table_desc, dpCtx, acidOp);
        ltd.setReplace(!qb.getParseInfo().isInsertIntoTable(dest_tab.getDbName(),
            dest_tab.getTableName()));
        ltd.setLbCtx(lbCtx);

        if (holdDDLTime) {
          LOG.info("this query will not update transient_lastDdlTime!");
          ltd.setHoldDDLTime(true);
        }
        loadTableWork.add(ltd);
      }

      WriteEntity output = null;

      // Here only register the whole table for post-exec hook if no DP present
      // in the case of DP, we will register WriteEntity in MoveTask when the
      // list of dynamically created partitions are known.
      if ((dpCtx == null || dpCtx.getNumDPCols() == 0)) {
        output = new WriteEntity(dest_tab, determineWriteType(ltd, isNonNativeTable));
        if (!outputs.add(output)) {
          throw new SemanticException(ErrorMsg.OUTPUT_SPECIFIED_MULTIPLE_TIMES
              .getMsg(dest_tab.getTableName()));
        }
      }
      if ((dpCtx != null) && (dpCtx.getNumDPCols() >= 0)) {
        // No static partition specified
        if (dpCtx.getNumSPCols() == 0) {
          output = new WriteEntity(dest_tab, determineWriteType(ltd, isNonNativeTable), false);
          outputs.add(output);
        }
        // part of the partition specified
        // Create a DummyPartition in this case. Since, the metastore does not store partial
        // partitions currently, we need to store dummy partitions
        else {
          try {
            String ppath = dpCtx.getSPPath();
            ppath = ppath.substring(0, ppath.length() - 1);
            DummyPartition p =
                new DummyPartition(dest_tab, dest_tab.getDbName()
                    + "@" + dest_tab.getTableName() + "@" + ppath,
                    partSpec);
            output = new WriteEntity(p, WriteEntity.WriteType.INSERT, false);
            outputs.add(output);
          } catch (HiveException e) {
            throw new SemanticException(e.getMessage(), e);
          }
        }
      }

      ctx.getLoadTableOutputMap().put(ltd, output);
      break;
    }
    case QBMetaData.DEST_PARTITION: {

      dest_part = qbm.getDestPartitionForAlias(dest);
      dest_tab = dest_part.getTable();
      destTableIsAcid = isAcidTable(dest_tab);
      if ((!conf.getBoolVar(HiveConf.ConfVars.HIVE_INSERT_INTO_EXTERNAL_TABLES)) &&
          dest_tab.getTableType().equals(TableType.EXTERNAL_TABLE)) {
        throw new SemanticException(
            ErrorMsg.INSERT_EXTERNAL_TABLE.getMsg(dest_tab.getTableName()));
      }

      Path tabPath = dest_tab.getPath();
      Path partPath = dest_part.getDataLocation();

      // If the query here is an INSERT_INTO and the target is an immutable table,
      // verify that our destination is empty before proceeding
      if (dest_tab.isImmutable() &&
          qb.getParseInfo().isInsertIntoTable(dest_tab.getDbName(),dest_tab.getTableName())){
        qb.getParseInfo().isInsertToTable();
        try {
          FileSystem fs = partPath.getFileSystem(conf);
          if (! MetaStoreUtils.isDirEmpty(fs,partPath)){
            LOG.warn("Attempted write into an immutable table partition : "
                + dest_tab.getTableName() + " : " + partPath);
            throw new SemanticException(
                ErrorMsg.INSERT_INTO_IMMUTABLE_TABLE.getMsg(dest_tab.getTableName()));
          }
        } catch (IOException ioe) {
            LOG.warn("Error while trying to determine if immutable table partition has any data : "
                + dest_tab.getTableName() + " : " + partPath);
          throw new SemanticException(ErrorMsg.INSERT_INTO_IMMUTABLE_TABLE.getMsg(ioe.getMessage()));
        }
      }

      // if the table is in a different dfs than the partition,
      // replace the partition's dfs with the table's dfs.
      dest_path = new Path(tabPath.toUri().getScheme(), tabPath.toUri()
          .getAuthority(), partPath.toUri().getPath());

      queryTmpdir = ctx.getExternalTmpPath(dest_path);
      table_desc = Utilities.getTableDesc(dest_tab);

      // Add sorting/bucketing if needed
      input = genBucketingSortingDest(dest, input, qb, table_desc, dest_tab, rsCtx);

      idToTableNameMap.put(String.valueOf(destTableId), dest_tab.getTableName());
      currentTableId = destTableId;
      destTableId++;

      lbCtx = constructListBucketingCtx(dest_part.getSkewedColNames(),
          dest_part.getSkewedColValues(), dest_part.getSkewedColValueLocationMaps(),
          dest_part.isStoredAsSubDirectories(), conf);
      AcidUtils.Operation acidOp = AcidUtils.Operation.NOT_ACID;
      if (destTableIsAcid) {
        acidOp = getAcidType(table_desc.getOutputFileFormatClass());
        checkAcidConstraints(qb, table_desc, dest_tab);
      }
      ltd = new LoadTableDesc(queryTmpdir, table_desc, dest_part.getSpec(), acidOp);
      ltd.setReplace(!qb.getParseInfo().isInsertIntoTable(dest_tab.getDbName(),
          dest_tab.getTableName()));
      ltd.setLbCtx(lbCtx);

      if (holdDDLTime) {
        try {
          Partition part = db.getPartition(dest_tab, dest_part.getSpec(), false);
          if (part == null) {
            throw new SemanticException(generateErrorMessage(
                qb.getParseInfo().getDestForClause(dest),
                ErrorMsg.HOLD_DDLTIME_ON_NONEXIST_PARTITIONS.getMsg()));
          }
        } catch (HiveException e) {
          throw new SemanticException(e);
        }
        LOG.info("this query will not update transient_lastDdlTime!");
        ltd.setHoldDDLTime(true);
      }
      loadTableWork.add(ltd);
      if (!outputs.add(new WriteEntity(dest_part, (ltd.getReplace() ?
          WriteEntity.WriteType.INSERT_OVERWRITE :
          WriteEntity.WriteType.INSERT)))) {
        throw new SemanticException(ErrorMsg.OUTPUT_SPECIFIED_MULTIPLE_TIMES
            .getMsg(dest_tab.getTableName() + "@" + dest_part.getName()));
      }
      break;
    }
    case QBMetaData.DEST_LOCAL_FILE:
      isLocal = true;
      // fall through
    case QBMetaData.DEST_DFS_FILE: {
      dest_path = new Path(qbm.getDestFileForAlias(dest));

      if (isLocal) {
        // for local directory - we always write to map-red intermediate
        // store and then copy to local fs
        queryTmpdir = ctx.getMRTmpPath();
      } else {
        // otherwise write to the file system implied by the directory
        // no copy is required. we may want to revisit this policy in future

        try {
          Path qPath = FileUtils.makeQualified(dest_path, conf);
          queryTmpdir = ctx.getExtTmpPathRelTo(qPath);
        } catch (Exception e) {
          throw new SemanticException("Error creating temporary folder on: "
              + dest_path, e);
        }
      }
      String cols = "";
      String colTypes = "";
      ArrayList<ColumnInfo> colInfos = inputRR.getColumnInfos();

      // CTAS case: the file output format and serde are defined by the create
      // table command rather than taking the default value
      List<FieldSchema> field_schemas = null;
      CreateTableDesc tblDesc = qb.getTableDesc();
      if (tblDesc != null) {
        field_schemas = new ArrayList<FieldSchema>();
      }

      boolean first = true;
      for (ColumnInfo colInfo : colInfos) {
        String[] nm = inputRR.reverseLookup(colInfo.getInternalName());

        if (nm[1] != null) { // non-null column alias
          colInfo.setAlias(nm[1]);
        }

        String colName = colInfo.getInternalName();  //default column name
        if (field_schemas != null) {
          FieldSchema col = new FieldSchema();
          if (!("".equals(nm[0])) && nm[1] != null) {
            colName = unescapeIdentifier(colInfo.getAlias()).toLowerCase(); // remove ``
          }
          colName = fixCtasColumnName(colName);
          col.setName(colName);
          col.setType(colInfo.getType().getTypeName());
          field_schemas.add(col);
        }

        if (!first) {
          cols = cols.concat(",");
          colTypes = colTypes.concat(":");
        }

        first = false;
        cols = cols.concat(colName);

        // Replace VOID type with string when the output is a temp table or
        // local files.
        // A VOID type can be generated under the query:
        //
        // select NULL from tt;
        // or
        // insert overwrite local directory "abc" select NULL from tt;
        //
        // where there is no column type to which the NULL value should be
        // converted.
        //
        String tName = colInfo.getType().getTypeName();
        if (tName.equals(serdeConstants.VOID_TYPE_NAME)) {
          colTypes = colTypes.concat(serdeConstants.STRING_TYPE_NAME);
        } else {
          colTypes = colTypes.concat(tName);
        }
      }

      // update the create table descriptor with the resulting schema.
      if (tblDesc != null) {
        tblDesc.setCols(new ArrayList<FieldSchema>(field_schemas));
      }

      boolean isDestTempFile = true;
      if (!ctx.isMRTmpFileURI(dest_path.toUri().toString())) {
        idToTableNameMap.put(String.valueOf(destTableId), dest_path.toUri().toString());
        currentTableId = destTableId;
        destTableId++;
        isDestTempFile = false;
      }

      boolean isDfsDir = (dest_type.intValue() == QBMetaData.DEST_DFS_FILE);
      loadFileWork.add(new LoadFileDesc(tblDesc, queryTmpdir, dest_path, isDfsDir, cols,
          colTypes));

      if (tblDesc == null) {
        if (qb.getIsQuery()) {
          String fileFormat = HiveConf.getVar(conf, HiveConf.ConfVars.HIVEQUERYRESULTFILEFORMAT);
          table_desc = PlanUtils.getDefaultQueryOutputTableDesc(cols, colTypes, fileFormat);
        } else {
          table_desc = PlanUtils.getDefaultTableDesc(qb.getLLocalDirectoryDesc(), cols, colTypes);
        }
      } else {
        table_desc = PlanUtils.getTableDesc(tblDesc, cols, colTypes);
      }

      if (!outputs.add(new WriteEntity(dest_path, !isDfsDir, isDestTempFile))) {
        throw new SemanticException(ErrorMsg.OUTPUT_SPECIFIED_MULTIPLE_TIMES
            .getMsg(dest_path.toUri().toString()));
      }
      break;
    }
    default:
      throw new SemanticException("Unknown destination type: " + dest_type);
    }

    input = genConversionSelectOperator(dest, qb, input, table_desc, dpCtx);
    inputRR = opParseCtx.get(input).getRowResolver();

    ArrayList<ColumnInfo> vecCol = new ArrayList<ColumnInfo>();

    if (updating() || deleting()) {
      vecCol.add(new ColumnInfo(VirtualColumn.ROWID.getName(), VirtualColumn.ROWID.getTypeInfo(),
          "", true));
    } else {
      try {
        StructObjectInspector rowObjectInspector = (StructObjectInspector) table_desc
            .getDeserializer(conf).getObjectInspector();
        List<? extends StructField> fields = rowObjectInspector
            .getAllStructFieldRefs();
        for (int i = 0; i < fields.size(); i++) {
          vecCol.add(new ColumnInfo(fields.get(i).getFieldName(), TypeInfoUtils
              .getTypeInfoFromObjectInspector(fields.get(i)
                  .getFieldObjectInspector()), "", false));
        }
      } catch (Exception e) {
        throw new SemanticException(e.getMessage(), e);
      }
    }

    RowSchema fsRS = new RowSchema(vecCol);

    // The output files of a FileSink can be merged if they are either not being written to a table
    // or are being written to a table which is either not bucketed or enforce bucketing is not set
    // and table the table is either not sorted or enforce sorting is not set
    boolean canBeMerged = (dest_tab == null || !((dest_tab.getNumBuckets() > 0 &&
        conf.getBoolVar(HiveConf.ConfVars.HIVEENFORCEBUCKETING)) ||
        (dest_tab.getSortCols() != null && dest_tab.getSortCols().size() > 0 &&
        conf.getBoolVar(HiveConf.ConfVars.HIVEENFORCESORTING))));

    // If this table is working with ACID semantics, turn off merging
    canBeMerged &= !destTableIsAcid;

    FileSinkDesc fileSinkDesc = new FileSinkDesc(
      queryTmpdir,
      table_desc,
      conf.getBoolVar(HiveConf.ConfVars.COMPRESSRESULT),
      currentTableId,
      rsCtx.isMultiFileSpray(),
      canBeMerged,
      rsCtx.getNumFiles(),
      rsCtx.getTotalFiles(),
      rsCtx.getPartnCols(),
      dpCtx);

    // If this is an insert, update, or delete on an ACID table then mark that so the
    // FileSinkOperator knows how to properly write to it.
    if (destTableIsAcid) {
      AcidUtils.Operation wt = updating() ? AcidUtils.Operation.UPDATE :
          (deleting() ? AcidUtils.Operation.DELETE : AcidUtils.Operation.INSERT);
      fileSinkDesc.setWriteType(wt);
      acidFileSinks.add(fileSinkDesc);
    }

    /* Set List Bucketing context. */
    if (lbCtx != null) {
      lbCtx.processRowSkewedIndex(fsRS);
      lbCtx.calculateSkewedValueSubDirList();
    }
    fileSinkDesc.setLbCtx(lbCtx);

    // set it in plan instead of runtime in FileSinkOperator
    fileSinkDesc.setStatsCollectRawDataSize(HiveConf.getBoolVar(conf,
        HiveConf.ConfVars.HIVE_STATS_COLLECT_RAWDATASIZE));

    // set the stats publishing/aggregating key prefix
    // the same as directory name. The directory name
    // can be changed in the optimizer but the key should not be changed
    // it should be the same as the MoveWork's sourceDir.
    fileSinkDesc.setStatsAggPrefix(fileSinkDesc.getDirName().toString());
    if (HiveConf.getVar(conf, HIVESTATSDBCLASS).equalsIgnoreCase(StatDB.fs.name())) {
      String statsTmpLoc = ctx.getExtTmpPathRelTo(queryTmpdir).toString();
      LOG.info("Set stats collection dir : " + statsTmpLoc);
      conf.set(StatsSetupConst.STATS_TMP_LOC, statsTmpLoc);
    }

    if (dest_part != null) {
      try {
        String staticSpec = Warehouse.makePartPath(dest_part.getSpec());
        fileSinkDesc.setStaticSpec(staticSpec);
      } catch (MetaException e) {
        throw new SemanticException(e);
      }
    } else if (dpCtx != null) {
      fileSinkDesc.setStaticSpec(dpCtx.getSPPath());
    }

    Operator output = putOpInsertMap(OperatorFactory.getAndMakeChild(fileSinkDesc,
        fsRS, input), inputRR);

    if (ltd != null && SessionState.get() != null) {
      SessionState.get().getLineageState()
          .mapDirToFop(ltd.getSourcePath(), (FileSinkOperator) output);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Created FileSink Plan for clause: " + dest + "dest_path: "
          + dest_path + " row schema: " + inputRR.toString());
    }

    FileSinkOperator fso = (FileSinkOperator) output;
    fso.getConf().setTable(dest_tab);
    fsopToTable.put(fso, dest_tab);
    return output;
  }

  String fixCtasColumnName(String colName) {
    return colName;
  }

  // Check constraints on acid tables.  This includes
  // * no insert overwrites
  // * no use of vectorization
  // * turns off reduce deduplication optimization, as that sometimes breaks acid
  // * Check that the table is bucketed
  // * Check that the table is not sorted
  // This method assumes you have already decided that this is an Acid write.  Don't call it if
  // that isn't true.
  private void checkAcidConstraints(QB qb, TableDesc tableDesc,
                                    Table table) throws SemanticException {
    String tableName = tableDesc.getTableName();
    if (!qb.getParseInfo().isInsertIntoTable(tableName)) {
      LOG.debug("Couldn't find table " + tableName + " in insertIntoTable");
      throw new SemanticException(ErrorMsg.NO_INSERT_OVERWRITE_WITH_ACID.getMsg());
    }
    if (conf.getBoolVar(ConfVars.HIVE_VECTORIZATION_ENABLED)) {
      LOG.info("Turning off vectorization for acid write operation");
      conf.setBoolVar(ConfVars.HIVE_VECTORIZATION_ENABLED, false);
    }
    LOG.info("Modifying config values for ACID write");
    conf.setBoolVar(ConfVars.HIVEOPTREDUCEDEDUPLICATION, true);
    conf.setIntVar(ConfVars.HIVEOPTREDUCEDEDUPLICATIONMINREDUCER, 1);
    conf.setBoolVar(ConfVars.HIVE_HADOOP_SUPPORTS_SUBDIRECTORIES, true);
    conf.set(AcidUtils.CONF_ACID_KEY, "true");
    conf.setBoolVar(ConfVars.HIVEOPTSORTDYNAMICPARTITION, false);

    if (table.getNumBuckets() < 1) {
      throw new SemanticException(ErrorMsg.ACID_OP_ON_NONACID_TABLE, table.getTableName());
    }
    if (table.getSortCols() != null && table.getSortCols().size() > 0) {
      throw new SemanticException(ErrorMsg.ACID_NO_SORTED_BUCKETS, table.getTableName());
    }



  }

  /**
   * Generate the conversion SelectOperator that converts the columns into the
   * types that are expected by the table_desc.
   */
  Operator genConversionSelectOperator(String dest, QB qb, Operator input,
      TableDesc table_desc, DynamicPartitionCtx dpCtx) throws SemanticException {
    StructObjectInspector oi = null;
    try {
      Deserializer deserializer = table_desc.getDeserializerClass()
          .newInstance();
      SerDeUtils.initializeSerDe(deserializer, conf, table_desc.getProperties(), null);
      oi = (StructObjectInspector) deserializer.getObjectInspector();
    } catch (Exception e) {
      throw new SemanticException(e);
    }

    // Check column number
    List<? extends StructField> tableFields = oi.getAllStructFieldRefs();
    boolean dynPart = HiveConf.getBoolVar(conf, HiveConf.ConfVars.DYNAMICPARTITIONING);
    ArrayList<ColumnInfo> rowFields = opParseCtx.get(input).getRowResolver()
        .getColumnInfos();
    int inColumnCnt = rowFields.size();
    int outColumnCnt = tableFields.size();
    if (dynPart && dpCtx != null) {
      outColumnCnt += dpCtx.getNumDPCols();
    }

    if (deleting()) {
      // Figure out if we have partition columns in the list or not.  If so,
      // add them into the mapping.  Partition columns will be located after the row id.
      if (rowFields.size() > 1) {
        // This means we have partition columns to deal with, so set up the mapping from the
        // input to the partition columns.
        dpCtx.mapInputToDP(rowFields.subList(1, rowFields.size()));
      }
    } else if (updating()) {
      // In this case we expect the number of in fields to exceed the number of out fields by one
      // (for the ROW__ID virtual column).  If there are more columns than this,
      // then the extras are for dynamic partitioning
      if (dynPart && dpCtx != null) {
        dpCtx.mapInputToDP(rowFields.subList(tableFields.size() + 1, rowFields.size()));
      }
    } else {
      if (inColumnCnt != outColumnCnt) {
        String reason = "Table " + dest + " has " + outColumnCnt
            + " columns, but query has " + inColumnCnt + " columns.";
        throw new SemanticException(ErrorMsg.TARGET_TABLE_COLUMN_MISMATCH.getMsg(
            qb.getParseInfo().getDestForClause(dest), reason));
      } else if (dynPart && dpCtx != null) {
        // create the mapping from input ExprNode to dest table DP column
        dpCtx.mapInputToDP(rowFields.subList(tableFields.size(), rowFields.size()));
      }
    }


    // Check column types
    boolean converted = false;
    int columnNumber = tableFields.size();
    ArrayList<ExprNodeDesc> expressions = new ArrayList<ExprNodeDesc>(
        columnNumber);

    // MetadataTypedColumnsetSerDe does not need type conversions because it
    // does the conversion to String by itself.
    boolean isMetaDataSerDe = table_desc.getDeserializerClass().equals(
        MetadataTypedColumnsetSerDe.class);
    boolean isLazySimpleSerDe = table_desc.getDeserializerClass().equals(
        LazySimpleSerDe.class);
    if (!isMetaDataSerDe && !deleting()) {

      // If we're updating, add the ROW__ID expression, then make the following column accesses
      // offset by 1 so that we don't try to convert the ROW__ID
      if (updating()) {
        expressions.add(new ExprNodeColumnDesc(rowFields.get(0).getType(),
            rowFields.get(0).getInternalName(), "", true));
      }

      // here only deals with non-partition columns. We deal with partition columns next
      for (int i = 0; i < columnNumber; i++) {
        int rowFieldsOffset = updating() ? i + 1 : i;
        ObjectInspector tableFieldOI = tableFields.get(i)
            .getFieldObjectInspector();
        TypeInfo tableFieldTypeInfo = TypeInfoUtils
            .getTypeInfoFromObjectInspector(tableFieldOI);
        TypeInfo rowFieldTypeInfo = rowFields.get(rowFieldsOffset).getType();
        ExprNodeDesc column = new ExprNodeColumnDesc(rowFieldTypeInfo,
            rowFields.get(rowFieldsOffset).getInternalName(), "", false,
            rowFields.get(rowFieldsOffset).isSkewedCol());
        // LazySimpleSerDe can convert any types to String type using
        // JSON-format.
        if (!tableFieldTypeInfo.equals(rowFieldTypeInfo)
            && !(isLazySimpleSerDe
                && tableFieldTypeInfo.getCategory().equals(Category.PRIMITIVE) && tableFieldTypeInfo
                  .equals(TypeInfoFactory.stringTypeInfo))) {
          // need to do some conversions here
          converted = true;
          if (tableFieldTypeInfo.getCategory() != Category.PRIMITIVE) {
            // cannot convert to complex types
            column = null;
          } else {
            column = ParseUtils.createConversionCast(
                column, (PrimitiveTypeInfo)tableFieldTypeInfo);
          }
          if (column == null) {
            String reason = "Cannot convert column " + i + " from "
                + rowFieldTypeInfo + " to " + tableFieldTypeInfo + ".";
            throw new SemanticException(ErrorMsg.TARGET_TABLE_COLUMN_MISMATCH
                .getMsg(qb.getParseInfo().getDestForClause(dest), reason));
          }
        }
        expressions.add(column);
      }
    }

    // deal with dynamic partition columns: convert ExprNodeDesc type to String??
    if (dynPart && dpCtx != null && dpCtx.getNumDPCols() > 0) {
      // DP columns starts with tableFields.size()
      for (int i = tableFields.size() + (updating() ? 1 : 0); i < rowFields.size(); ++i) {
        TypeInfo rowFieldTypeInfo = rowFields.get(i).getType();
        ExprNodeDesc column = new ExprNodeColumnDesc(
            rowFieldTypeInfo, rowFields.get(i).getInternalName(), "", false);
        expressions.add(column);
      }
      // converted = true; // [TODO]: should we check & convert type to String and set it to true?
    }

    if (converted) {
      // add the select operator
      RowResolver rowResolver = new RowResolver();
      ArrayList<String> colNames = new ArrayList<String>();
      Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();
      for (int i = 0; i < expressions.size(); i++) {
        String name = getColumnInternalName(i);
        rowResolver.put("", name, new ColumnInfo(name, expressions.get(i)
            .getTypeInfo(), "", false));
        colNames.add(name);
        colExprMap.put(name, expressions.get(i));
      }
      Operator output = putOpInsertMap(OperatorFactory.getAndMakeChild(
          new SelectDesc(expressions, colNames), new RowSchema(rowResolver
              .getColumnInfos()), input), rowResolver);
      output.setColumnExprMap(colExprMap);
      return output;
    } else {
      // not converted
      return input;
    }
  }

  @SuppressWarnings("nls")
  private Operator genLimitPlan(String dest, QB qb, Operator input, int limit)
      throws SemanticException {
    // A map-only job can be optimized - instead of converting it to a
    // map-reduce job, we can have another map
    // job to do the same to avoid the cost of sorting in the map-reduce phase.
    // A better approach would be to
    // write into a local file and then have a map-only job.
    // Add the limit operator to get the value fields

    RowResolver inputRR = opParseCtx.get(input).getRowResolver();

    LimitDesc limitDesc = new LimitDesc(limit);
    globalLimitCtx.setLastReduceLimitDesc(limitDesc);

    Operator limitMap = putOpInsertMap(OperatorFactory.getAndMakeChild(
        limitDesc, new RowSchema(inputRR.getColumnInfos()), input),
        inputRR);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Created LimitOperator Plan for clause: " + dest
          + " row schema: " + inputRR.toString());
    }

    return limitMap;
  }

  private Operator genUDTFPlan(GenericUDTF genericUDTF,
      String outputTableAlias, ArrayList<String> colAliases, QB qb,
      Operator input, boolean outerLV) throws SemanticException {

    // No GROUP BY / DISTRIBUTE BY / SORT BY / CLUSTER BY
    QBParseInfo qbp = qb.getParseInfo();
    if (!qbp.getDestToGroupBy().isEmpty()) {
      throw new SemanticException(ErrorMsg.UDTF_NO_GROUP_BY.getMsg());
    }
    if (!qbp.getDestToDistributeBy().isEmpty()) {
      throw new SemanticException(ErrorMsg.UDTF_NO_DISTRIBUTE_BY.getMsg());
    }
    if (!qbp.getDestToSortBy().isEmpty()) {
      throw new SemanticException(ErrorMsg.UDTF_NO_SORT_BY.getMsg());
    }
    if (!qbp.getDestToClusterBy().isEmpty()) {
      throw new SemanticException(ErrorMsg.UDTF_NO_CLUSTER_BY.getMsg());
    }
    if (!qbp.getAliasToLateralViews().isEmpty()) {
      throw new SemanticException(ErrorMsg.UDTF_LATERAL_VIEW.getMsg());
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Table alias: " + outputTableAlias + " Col aliases: "
          + colAliases);
    }

    // Use the RowResolver from the input operator to generate a input
    // ObjectInspector that can be used to initialize the UDTF. Then, the

    // resulting output object inspector can be used to make the RowResolver
    // for the UDTF operator
    RowResolver selectRR = opParseCtx.get(input).getRowResolver();
    ArrayList<ColumnInfo> inputCols = selectRR.getColumnInfos();

    // Create the object inspector for the input columns and initialize the UDTF
    ArrayList<String> colNames = new ArrayList<String>();
    ObjectInspector[] colOIs = new ObjectInspector[inputCols.size()];
    for (int i = 0; i < inputCols.size(); i++) {
      colNames.add(inputCols.get(i).getInternalName());
      colOIs[i] = inputCols.get(i).getObjectInspector();
    }
    StandardStructObjectInspector rowOI =
        ObjectInspectorFactory.getStandardStructObjectInspector(colNames, Arrays.asList(colOIs));
    StructObjectInspector outputOI = genericUDTF.initialize(rowOI);

    int numUdtfCols = outputOI.getAllStructFieldRefs().size();
    if (colAliases.isEmpty()) {
      // user did not specfied alias names, infer names from outputOI
      for (StructField field : outputOI.getAllStructFieldRefs()) {
        colAliases.add(field.getFieldName());
      }
    }
    // Make sure that the number of column aliases in the AS clause matches
    // the number of columns output by the UDTF
    int numSuppliedAliases = colAliases.size();
    if (numUdtfCols != numSuppliedAliases) {
      throw new SemanticException(ErrorMsg.UDTF_ALIAS_MISMATCH
          .getMsg("expected " + numUdtfCols + " aliases " + "but got "
              + numSuppliedAliases));
    }

    // Generate the output column info's / row resolver using internal names.
    ArrayList<ColumnInfo> udtfCols = new ArrayList<ColumnInfo>();

    Iterator<String> colAliasesIter = colAliases.iterator();
    for (StructField sf : outputOI.getAllStructFieldRefs()) {

      String colAlias = colAliasesIter.next();
      assert (colAlias != null);

      // Since the UDTF operator feeds into a LVJ operator that will rename
      // all the internal names, we can just use field name from the UDTF's OI
      // as the internal name
      ColumnInfo col = new ColumnInfo(sf.getFieldName(), TypeInfoUtils
          .getTypeInfoFromObjectInspector(sf.getFieldObjectInspector()),
          outputTableAlias, false);
      udtfCols.add(col);
    }

    // Create the row resolver for this operator from the output columns
    RowResolver out_rwsch = new RowResolver();
    for (int i = 0; i < udtfCols.size(); i++) {
      out_rwsch.put(outputTableAlias, colAliases.get(i), udtfCols.get(i));
    }

    // Add the UDTFOperator to the operator DAG
    Operator<?> udtf = putOpInsertMap(OperatorFactory.getAndMakeChild(
        new UDTFDesc(genericUDTF, outerLV), new RowSchema(out_rwsch.getColumnInfos()),
        input), out_rwsch);
    return udtf;
  }

  @SuppressWarnings("nls")
  private Operator genLimitMapRedPlan(String dest, QB qb, Operator input,
      int limit, boolean extraMRStep) throws SemanticException {
    // A map-only job can be optimized - instead of converting it to a
    // map-reduce job, we can have another map
    // job to do the same to avoid the cost of sorting in the map-reduce phase.
    // A better approach would be to
    // write into a local file and then have a map-only job.
    // Add the limit operator to get the value fields
    Operator curr = genLimitPlan(dest, qb, input, limit);

    // the client requested that an extra map-reduce step be performed
    if (!extraMRStep) {
      return curr;
    }

    // Create a reduceSink operator followed by another limit
    curr = genReduceSinkPlan(dest, qb, curr, 1, false);
    return genLimitPlan(dest, qb, curr, limit);
  }

  private ArrayList<ExprNodeDesc> getPartitionColsFromBucketCols(String dest, QB qb, Table tab,
      TableDesc table_desc, Operator input, boolean convert)
      throws SemanticException {
    List<String> tabBucketCols = tab.getBucketCols();
    List<FieldSchema> tabCols = tab.getCols();

    // Partition by the bucketing column
    List<Integer> posns = new ArrayList<Integer>();

    for (String bucketCol : tabBucketCols) {
      int pos = 0;
      for (FieldSchema tabCol : tabCols) {
        if (bucketCol.equals(tabCol.getName())) {
          posns.add(pos);
          break;
        }
        pos++;
      }
    }

    return genConvertCol(dest, qb, tab, table_desc, input, posns, convert);
  }

  // We have to set up the bucketing columns differently for update and deletes,
  // as it is always using the ROW__ID column.
  private ArrayList<ExprNodeDesc> getPartitionColsFromBucketColsForUpdateDelete(
      Operator input, boolean convert) throws SemanticException {
    //return genConvertCol(dest, qb, tab, table_desc, input, Arrays.asList(0), convert);
    // In the case of update and delete the bucketing column is always the first column,
    // and it isn't in the table info.  So rather than asking the table for it,
    // we'll construct it ourself and send it back.  This is based on the work done in
    // genConvertCol below.
    ColumnInfo rowField = opParseCtx.get(input).getRowResolver().getColumnInfos().get(0);
    TypeInfo rowFieldTypeInfo = rowField.getType();
    ExprNodeDesc column = new ExprNodeColumnDesc(rowFieldTypeInfo, rowField.getInternalName(),
        rowField.getTabAlias(), true);
    if (convert) {
      column = ParseUtils.createConversionCast(column, TypeInfoFactory.intTypeInfo);
    }
    ArrayList<ExprNodeDesc> rlist = new ArrayList<ExprNodeDesc>(1);
    rlist.add(column);
    return rlist;
  }

  private ArrayList<ExprNodeDesc> genConvertCol(String dest, QB qb, Table tab,
      TableDesc table_desc, Operator input, List<Integer> posns, boolean convert)
      throws SemanticException {
    StructObjectInspector oi = null;
    try {
      Deserializer deserializer = table_desc.getDeserializerClass()
          .newInstance();
      SerDeUtils.initializeSerDe(deserializer, conf, table_desc.getProperties(), null);
      oi = (StructObjectInspector) deserializer.getObjectInspector();
    } catch (Exception e) {
      throw new SemanticException(e);
    }

    List<? extends StructField> tableFields = oi.getAllStructFieldRefs();
    ArrayList<ColumnInfo> rowFields = opParseCtx.get(input).getRowResolver()
        .getColumnInfos();

    // Check column type
    int columnNumber = posns.size();
    ArrayList<ExprNodeDesc> expressions = new ArrayList<ExprNodeDesc>(columnNumber);
    for (Integer posn : posns) {
      ObjectInspector tableFieldOI = tableFields.get(posn).getFieldObjectInspector();
      TypeInfo tableFieldTypeInfo = TypeInfoUtils.getTypeInfoFromObjectInspector(tableFieldOI);
      TypeInfo rowFieldTypeInfo = rowFields.get(posn).getType();
      ExprNodeDesc column = new ExprNodeColumnDesc(rowFieldTypeInfo,
          rowFields.get(posn).getInternalName(), rowFields.get(posn).getTabAlias(),
          rowFields.get(posn).getIsVirtualCol());

      if (convert && !tableFieldTypeInfo.equals(rowFieldTypeInfo)) {
        // need to do some conversions here
        if (tableFieldTypeInfo.getCategory() != Category.PRIMITIVE) {
          // cannot convert to complex types
          column = null;
        } else {
          column = ParseUtils.createConversionCast(
              column, (PrimitiveTypeInfo)tableFieldTypeInfo);
        }
        if (column == null) {
          String reason = "Cannot convert column " + posn + " from "
              + rowFieldTypeInfo + " to " + tableFieldTypeInfo + ".";
          throw new SemanticException(ErrorMsg.TARGET_TABLE_COLUMN_MISMATCH
              .getMsg(qb.getParseInfo().getDestForClause(dest), reason));
        }
      }
      expressions.add(column);
    }

    return expressions;
  }

  private ArrayList<ExprNodeDesc> getSortCols(String dest, QB qb, Table tab, TableDesc table_desc,
      Operator input, boolean convert)
      throws SemanticException {
    RowResolver inputRR = opParseCtx.get(input).getRowResolver();
    List<Order> tabSortCols = tab.getSortCols();
    List<FieldSchema> tabCols = tab.getCols();

    // Partition by the bucketing column
    List<Integer> posns = new ArrayList<Integer>();
    for (Order sortCol : tabSortCols) {
      int pos = 0;
      for (FieldSchema tabCol : tabCols) {
        if (sortCol.getCol().equals(tabCol.getName())) {
          ColumnInfo colInfo = inputRR.getColumnInfos().get(pos);
          posns.add(pos);
          break;
        }
        pos++;
      }
    }

    return genConvertCol(dest, qb, tab, table_desc, input, posns, convert);
  }

  private ArrayList<Integer> getSortOrders(String dest, QB qb, Table tab, Operator input)
      throws SemanticException {
    RowResolver inputRR = opParseCtx.get(input).getRowResolver();
    List<Order> tabSortCols = tab.getSortCols();
    List<FieldSchema> tabCols = tab.getCols();

    ArrayList<Integer> orders = new ArrayList<Integer>();
    for (Order sortCol : tabSortCols) {
      for (FieldSchema tabCol : tabCols) {
        if (sortCol.getCol().equals(tabCol.getName())) {
          orders.add(sortCol.getOrder());
          break;
        }
      }
    }
    return orders;
  }

  @SuppressWarnings("nls")
  private Operator genReduceSinkPlanForSortingBucketing(Table tab, Operator input,
      ArrayList<ExprNodeDesc> sortCols,
      List<Integer> sortOrders,
      ArrayList<ExprNodeDesc> partitionCols,
      int numReducers)
      throws SemanticException {
    RowResolver inputRR = opParseCtx.get(input).getRowResolver();

    // For the generation of the values expression just get the inputs
    // signature and generate field expressions for those
    Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();
    ArrayList<ExprNodeDesc> valueCols = new ArrayList<ExprNodeDesc>();
    ArrayList<String> outputColumns = new ArrayList<String>();
    int i = 0;
    for (ColumnInfo colInfo : inputRR.getColumnInfos()) {
      String internalName = getColumnInternalName(i++);
      outputColumns.add(internalName);
      valueCols.add(new ExprNodeColumnDesc(colInfo));
      colExprMap.put(internalName, valueCols
          .get(valueCols.size() - 1));
    }

    StringBuilder order = new StringBuilder();
    for (int sortOrder : sortOrders) {
      order.append(sortOrder == BaseSemanticAnalyzer.HIVE_COLUMN_ORDER_ASC ? '+' : '-');
    }

    AcidUtils.Operation acidOp = (isAcidTable(tab) ? getAcidType() : AcidUtils.Operation.NOT_ACID);

    Operator interim = putOpInsertMap(OperatorFactory.getAndMakeChild(PlanUtils
        .getReduceSinkDesc(sortCols, valueCols, outputColumns, false, -1,
            partitionCols, order.toString(), numReducers, acidOp),
        new RowSchema(inputRR.getColumnInfos()), input), inputRR);
    interim.setColumnExprMap(colExprMap);
    reduceSinkOperatorsAddedByEnforceBucketingSorting.add((ReduceSinkOperator) interim);

    // Add the extract operator to get the value fields
    RowResolver out_rwsch = new RowResolver();
    RowResolver interim_rwsch = inputRR;
    Integer pos = Integer.valueOf(0);
    for (ColumnInfo colInfo : interim_rwsch.getColumnInfos()) {
      String[] info = interim_rwsch.reverseLookup(colInfo.getInternalName());
      out_rwsch.put(info[0], info[1], new ColumnInfo(
          getColumnInternalName(pos), colInfo.getType(), info[0],
          colInfo.getIsVirtualCol(), colInfo.isHiddenVirtualCol()));
      pos = Integer.valueOf(pos.intValue() + 1);
    }

    Operator output = putOpInsertMap(OperatorFactory.getAndMakeChild(
        new ExtractDesc(new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo,
            Utilities.ReduceField.VALUE.toString(), "", false)), new RowSchema(
            out_rwsch.getColumnInfos()), interim), out_rwsch);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Created ReduceSink Plan for table: " + tab.getTableName() +
          " row schema: " + out_rwsch.toString());
    }

    return output;

  }

  private Operator genReduceSinkPlan(String dest, QB qb, Operator<?> input,
      int numReducers, boolean hasOrderBy) throws SemanticException {
    RowResolver inputRR = opParseCtx.get(input).getRowResolver();

    // First generate the expression for the partition and sort keys
    // The cluster by clause / distribute by clause has the aliases for
    // partition function
    ASTNode partitionExprs = qb.getParseInfo().getClusterByForClause(dest);
    if (partitionExprs == null) {
      partitionExprs = qb.getParseInfo().getDistributeByForClause(dest);
    }
    ArrayList<ExprNodeDesc> partCols = new ArrayList<ExprNodeDesc>();
    if (partitionExprs != null) {
      int ccount = partitionExprs.getChildCount();
      for (int i = 0; i < ccount; ++i) {
        ASTNode cl = (ASTNode) partitionExprs.getChild(i);
        partCols.add(genExprNodeDesc(cl, inputRR));
      }
    }
    ASTNode sortExprs = qb.getParseInfo().getClusterByForClause(dest);
    if (sortExprs == null) {
      sortExprs = qb.getParseInfo().getSortByForClause(dest);
    }

    if (sortExprs == null) {
      sortExprs = qb.getParseInfo().getOrderByForClause(dest);
      if (sortExprs != null) {
        assert numReducers == 1;
        // in strict mode, in the presence of order by, limit must be specified
        Integer limit = qb.getParseInfo().getDestLimit(dest);
        if (conf.getVar(HiveConf.ConfVars.HIVEMAPREDMODE).equalsIgnoreCase(
            "strict")
            && limit == null) {
          throw new SemanticException(generateErrorMessage(sortExprs,
              ErrorMsg.NO_LIMIT_WITH_ORDERBY.getMsg()));
        }
      }
    }
    ArrayList<ExprNodeDesc> sortCols = new ArrayList<ExprNodeDesc>();
    StringBuilder order = new StringBuilder();
    if (sortExprs != null) {
      int ccount = sortExprs.getChildCount();
      for (int i = 0; i < ccount; ++i) {
        ASTNode cl = (ASTNode) sortExprs.getChild(i);

        if (cl.getType() == HiveParser.TOK_TABSORTCOLNAMEASC) {
          // SortBy ASC
          order.append("+");
          cl = (ASTNode) cl.getChild(0);
        } else if (cl.getType() == HiveParser.TOK_TABSORTCOLNAMEDESC) {
          // SortBy DESC
          order.append("-");
          cl = (ASTNode) cl.getChild(0);
        } else {
          // ClusterBy
          order.append("+");
        }
        ExprNodeDesc exprNode = genExprNodeDesc(cl, inputRR);
        sortCols.add(exprNode);
      }
    }
    Operator result = genReduceSinkPlan(
        input, partCols, sortCols, order.toString(), numReducers);
    if (result.getParentOperators().size() == 1 &&
        result.getParentOperators().get(0) instanceof ReduceSinkOperator) {
      ((ReduceSinkOperator) result.getParentOperators().get(0))
          .getConf().setHasOrderBy(hasOrderBy);
    }
    return result;
  }

  @SuppressWarnings("nls")
  private Operator genReduceSinkPlan(Operator<?> input,
      ArrayList<ExprNodeDesc> partitionCols, ArrayList<ExprNodeDesc> sortCols,
      String sortOrder, int numReducers) throws SemanticException {

    RowResolver inputRR = opParseCtx.get(input).getRowResolver();

    Operator dummy = Operator.createDummy();
    dummy.setParentOperators(Arrays.asList(input));

    ArrayList<ExprNodeDesc> sortColsBack = new ArrayList<ExprNodeDesc>();
    for (ExprNodeDesc sortCol : sortCols) {
      sortColsBack.add(ExprNodeDescUtils.backtrack(sortCol, dummy, input));
    }
    // For the generation of the values expression just get the inputs
    // signature and generate field expressions for those
    RowResolver rsRR = new RowResolver();
    ArrayList<String> outputColumns = new ArrayList<String>();
    ArrayList<ExprNodeDesc> valueCols = new ArrayList<ExprNodeDesc>();
    ArrayList<ExprNodeDesc> valueColsBack = new ArrayList<ExprNodeDesc>();
    Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();

    ArrayList<ColumnInfo> columnInfos = inputRR.getColumnInfos();

    int[] index = new int[columnInfos.size()];
    for (int i = 0; i < index.length; i++) {
      ColumnInfo colInfo = columnInfos.get(i);
      String[] nm = inputRR.reverseLookup(colInfo.getInternalName());
      String[] nm2 = inputRR.getAlternateMappings(colInfo.getInternalName());
      ExprNodeColumnDesc value = new ExprNodeColumnDesc(colInfo);

      // backtrack can be null when input is script operator
      ExprNodeDesc valueBack = ExprNodeDescUtils.backtrack(value, dummy, input);
      int kindex = valueBack == null ? -1 : ExprNodeDescUtils.indexOf(valueBack, sortColsBack);
      if (kindex >= 0) {
        index[i] = kindex;
        ColumnInfo newColInfo = new ColumnInfo(colInfo);
        newColInfo.setInternalName(Utilities.ReduceField.KEY + ".reducesinkkey" + kindex);
        newColInfo.setTabAlias(nm[0]);
        rsRR.addMappingOnly(nm[0], nm[1], newColInfo);
        if (nm2 != null) {
          rsRR.addMappingOnly(nm2[0], nm2[1], newColInfo);
        }
        continue;
      }
      int vindex = valueBack == null ? -1 : ExprNodeDescUtils.indexOf(valueBack, valueColsBack);
      if (vindex >= 0) {
        index[i] = -vindex - 1;
        continue;
      }
      index[i] = -valueCols.size() - 1;
      String outputColName = getColumnInternalName(valueCols.size());

      valueCols.add(value);
      valueColsBack.add(valueBack);

      ColumnInfo newColInfo = new ColumnInfo(colInfo);
      newColInfo.setInternalName(Utilities.ReduceField.VALUE + "." + outputColName);
      newColInfo.setTabAlias(nm[0]);

      rsRR.put(nm[0], nm[1], newColInfo);
      if (nm2 != null) {
        rsRR.addMappingOnly(nm2[0], nm2[1], newColInfo);
      }
      outputColumns.add(outputColName);
    }

    dummy.setParentOperators(null);

    // TODO Not 100% sure NOT_ACID is always right here.
    ReduceSinkDesc rsdesc = PlanUtils.getReduceSinkDesc(sortCols, valueCols, outputColumns,
        false, -1, partitionCols, sortOrder, numReducers, AcidUtils.Operation.NOT_ACID);
    Operator interim = putOpInsertMap(OperatorFactory.getAndMakeChild(rsdesc,
        new RowSchema(rsRR.getColumnInfos()), input), rsRR);

    List<String> keyColNames = rsdesc.getOutputKeyColumnNames();
    for (int i = 0 ; i < keyColNames.size(); i++) {
      colExprMap.put(Utilities.ReduceField.KEY + "." + keyColNames.get(i), sortCols.get(i));
    }
    List<String> valueColNames = rsdesc.getOutputValueColumnNames();
    for (int i = 0 ; i < valueColNames.size(); i++) {
      colExprMap.put(Utilities.ReduceField.VALUE + "." + valueColNames.get(i), valueCols.get(i));
    }
    interim.setColumnExprMap(colExprMap);

    RowResolver selectRR = new RowResolver();
    ArrayList<ExprNodeDesc> selCols = new ArrayList<ExprNodeDesc>();
    ArrayList<String> selOutputCols = new ArrayList<String>();
    Map<String, ExprNodeDesc> selColExprMap = new HashMap<String, ExprNodeDesc>();

    for (int i = 0; i < index.length; i++) {
      ColumnInfo prev = columnInfos.get(i);
      String[] nm = inputRR.reverseLookup(prev.getInternalName());
      String[] nm2 = inputRR.getAlternateMappings(prev.getInternalName());
      ColumnInfo info = new ColumnInfo(prev);

      String field;
      if (index[i] >= 0) {
        field = Utilities.ReduceField.KEY + "." + keyColNames.get(index[i]);
      } else {
        field = Utilities.ReduceField.VALUE + "." + valueColNames.get(-index[i] - 1);
      }
      String internalName = getColumnInternalName(i);
      ExprNodeColumnDesc desc = new ExprNodeColumnDesc(info.getType(),
          field, info.getTabAlias(), info.getIsVirtualCol());
      selCols.add(desc);

      info.setInternalName(internalName);
      selectRR.put(nm[0], nm[1], info);
      if (nm2 != null) {
        selectRR.addMappingOnly(nm2[0], nm2[1], info);
      }
      selOutputCols.add(internalName);
      selColExprMap.put(internalName, desc);
    }
    SelectDesc select = new SelectDesc(selCols, selOutputCols);
    Operator output = putOpInsertMap(OperatorFactory.getAndMakeChild(select,
        new RowSchema(selectRR.getColumnInfos()), interim), selectRR);
    output.setColumnExprMap(selColExprMap);
    return output;
  }

  private Operator genJoinOperatorChildren(QBJoinTree join, Operator left,
      Operator[] right, HashSet<Integer> omitOpts, ExprNodeDesc[][] joinKeys) throws SemanticException {

    RowResolver outputRR = new RowResolver();
    ArrayList<String> outputColumnNames = new ArrayList<String>();
    // all children are base classes
    Operator<?>[] rightOps = new Operator[right.length];
    int outputPos = 0;

    Map<String, Byte> reversedExprs = new HashMap<String, Byte>();
    HashMap<Byte, List<ExprNodeDesc>> exprMap = new HashMap<Byte, List<ExprNodeDesc>>();
    Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();
    HashMap<Integer, Set<String>> posToAliasMap = new HashMap<Integer, Set<String>>();
    HashMap<Byte, List<ExprNodeDesc>> filterMap =
        new HashMap<Byte, List<ExprNodeDesc>>();

    for (int pos = 0; pos < right.length; ++pos) {
      Operator<?> input = right[pos] == null ? left : right[pos];
      if (input == null) {
        input = left;
      }
      ReduceSinkOperator rs = (ReduceSinkOperator) input;
      if (rs.getNumParent() != 1) {
        throw new SemanticException("RS should have single parent");
      }
      Operator<?> parent = rs.getParentOperators().get(0);
      ReduceSinkDesc rsDesc = (ReduceSinkDesc) (input.getConf());

      int[] index = rs.getValueIndex();

      ArrayList<ExprNodeDesc> valueDesc = new ArrayList<ExprNodeDesc>();
      ArrayList<ExprNodeDesc> filterDesc = new ArrayList<ExprNodeDesc>();
      Byte tag = (byte) rsDesc.getTag();

      // check whether this input operator produces output
      if (omitOpts != null && omitOpts.contains(pos)) {
        exprMap.put(tag, valueDesc);
        filterMap.put(tag, filterDesc);
        rightOps[pos] = input;
        continue;
      }

      List<String> keyColNames = rsDesc.getOutputKeyColumnNames();
      List<String> valColNames = rsDesc.getOutputValueColumnNames();

      // prepare output descriptors for the input opt
      RowResolver inputRR = opParseCtx.get(input).getRowResolver();
      RowResolver parentRR = opParseCtx.get(parent).getRowResolver();
      posToAliasMap.put(pos, new HashSet<String>(inputRR.getTableNames()));

      List<ColumnInfo> columns = parentRR.getColumnInfos();
      for (int i = 0; i < index.length; i++) {
        ColumnInfo prev = columns.get(i);
        String[] nm = parentRR.reverseLookup(prev.getInternalName());
        String[] nm2 = parentRR.getAlternateMappings(prev.getInternalName());
        if (outputRR.get(nm[0], nm[1]) != null) {
          continue;
        }
        ColumnInfo info = new ColumnInfo(prev);
        String field;
        if (index[i] >= 0) {
          field = Utilities.ReduceField.KEY + "." + keyColNames.get(index[i]);
        } else {
          field = Utilities.ReduceField.VALUE + "." + valColNames.get(-index[i] - 1);
        }
        String internalName = getColumnInternalName(outputColumnNames.size());
        ExprNodeColumnDesc desc = new ExprNodeColumnDesc(info.getType(),
            field, info.getTabAlias(), info.getIsVirtualCol());

        info.setInternalName(internalName);
        colExprMap.put(internalName, desc);
        outputRR.put(nm[0], nm[1], info);
        if (nm2 != null) {
          outputRR.addMappingOnly(nm2[0], nm2[1], info);
        }

        valueDesc.add(desc);
        outputColumnNames.add(internalName);
        reversedExprs.put(internalName, tag);
      }
      for (ASTNode cond : join.getFilters().get(tag)) {
        filterDesc.add(genExprNodeDesc(cond, inputRR));
      }
      exprMap.put(tag, valueDesc);
      filterMap.put(tag, filterDesc);
      rightOps[pos] = input;
    }

    JoinCondDesc[] joinCondns = new JoinCondDesc[join.getJoinCond().length];
    for (int i = 0; i < join.getJoinCond().length; i++) {
      JoinCond condn = join.getJoinCond()[i];
      joinCondns[i] = new JoinCondDesc(condn);
    }

    JoinDesc desc = new JoinDesc(exprMap, outputColumnNames,
        join.getNoOuterJoin(), joinCondns, filterMap, joinKeys);
    desc.setReversedExprs(reversedExprs);
    desc.setFilterMap(join.getFilterMap());

    JoinOperator joinOp = (JoinOperator) OperatorFactory.getAndMakeChild(desc,
        new RowSchema(outputRR.getColumnInfos()), rightOps);
    joinOp.setColumnExprMap(colExprMap);
    joinOp.setPosToAliasMap(posToAliasMap);

    if (join.getNullSafes() != null) {
      boolean[] nullsafes = new boolean[join.getNullSafes().size()];
      for (int i = 0; i < nullsafes.length; i++) {
        nullsafes[i] = join.getNullSafes().get(i);
      }
      desc.setNullSafes(nullsafes);
    }
    return putOpInsertMap(joinOp, outputRR);
  }

  private ExprNodeDesc[][] genJoinKeys(QBJoinTree joinTree, Operator[] inputs)
      throws SemanticException {
    ExprNodeDesc[][] joinKeys = new ExprNodeDesc[inputs.length][];
    for (int i = 0; i < inputs.length; i++) {
      RowResolver inputRR = opParseCtx.get(inputs[i]).getRowResolver();
      List<ASTNode> expressions = joinTree.getExpressions().get(i);
      joinKeys[i] = new ExprNodeDesc[expressions.size()];
      for (int j = 0; j < joinKeys[i].length; j++) {
        joinKeys[i][j] = genExprNodeDesc(expressions.get(j), inputRR);
      }
    }
    // Type checking and implicit type conversion for join keys
    return genJoinOperatorTypeCheck(joinKeys);
  }

  @SuppressWarnings("nls")
  private Operator genJoinReduceSinkChild(QB qb, ExprNodeDesc[] joinKeys,
      Operator<?> child, String[] srcs, int tag) throws SemanticException {

    Operator dummy = Operator.createDummy();  // dummy for backtracking
    dummy.setParentOperators(Arrays.asList(child));

    RowResolver inputRR = opParseCtx.get(child).getRowResolver();
    RowResolver outputRR = new RowResolver();
    ArrayList<String> outputColumns = new ArrayList<String>();
    ArrayList<ExprNodeDesc> reduceKeys = new ArrayList<ExprNodeDesc>();
    ArrayList<ExprNodeDesc> reduceKeysBack = new ArrayList<ExprNodeDesc>();

    // Compute join keys and store in reduceKeys
    for (ExprNodeDesc joinKey : joinKeys) {
      reduceKeys.add(joinKey);
      reduceKeysBack.add(ExprNodeDescUtils.backtrack(joinKey, dummy, child));
    }

    // Walk over the input row resolver and copy in the output
    ArrayList<ExprNodeDesc> reduceValues = new ArrayList<ExprNodeDesc>();
    ArrayList<ExprNodeDesc> reduceValuesBack = new ArrayList<ExprNodeDesc>();
    Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();

    List<ColumnInfo> columns = inputRR.getColumnInfos();
    int[] index = new int[columns.size()];
    for (int i = 0; i < columns.size(); i++) {
      ColumnInfo colInfo = columns.get(i);
      String[] nm = inputRR.reverseLookup(colInfo.getInternalName());
      String[] nm2 = inputRR.getAlternateMappings(colInfo.getInternalName());
      ExprNodeDesc expr = new ExprNodeColumnDesc(colInfo);

      // backtrack can be null when input is script operator
      ExprNodeDesc exprBack = ExprNodeDescUtils.backtrack(expr, dummy, child);
      int kindex = exprBack == null ? -1 : ExprNodeDescUtils.indexOf(exprBack, reduceKeysBack);
      if (kindex >= 0) {
        ColumnInfo newColInfo = new ColumnInfo(colInfo);
        newColInfo.setInternalName(Utilities.ReduceField.KEY + ".reducesinkkey" + kindex);
        newColInfo.setTabAlias(nm[0]);
        outputRR.addMappingOnly(nm[0], nm[1], newColInfo);
        if (nm2 != null) {
          outputRR.addMappingOnly(nm2[0], nm2[1], newColInfo);
        }
        index[i] = kindex;
        continue;
      }
      int vindex = exprBack == null ? -1 : ExprNodeDescUtils.indexOf(exprBack, reduceValuesBack);
      if (kindex >= 0) {
        index[i] = -vindex - 1;
        continue;
      }
      index[i] = -reduceValues.size() - 1;
      String outputColName = getColumnInternalName(reduceValues.size());

      reduceValues.add(expr);
      reduceValuesBack.add(exprBack);

      ColumnInfo newColInfo = new ColumnInfo(colInfo);
      newColInfo.setInternalName(Utilities.ReduceField.VALUE + "." + outputColName);
      newColInfo.setTabAlias(nm[0]);

      outputRR.put(nm[0], nm[1], newColInfo);
      if (nm2 != null) {
        outputRR.addMappingOnly(nm2[0], nm2[1], newColInfo);
      }
      outputColumns.add(outputColName);
    }
    dummy.setParentOperators(null);

    int numReds = -1;

    // Use only 1 reducer in case of cartesian product
    if (reduceKeys.size() == 0) {
      numReds = 1;

      // Cartesian product is not supported in strict mode
      if (conf.getVar(HiveConf.ConfVars.HIVEMAPREDMODE).equalsIgnoreCase(
          "strict")) {
        throw new SemanticException(ErrorMsg.NO_CARTESIAN_PRODUCT.getMsg());
      }
    }

    ReduceSinkDesc rsDesc = PlanUtils.getReduceSinkDesc(reduceKeys,
        reduceValues, outputColumns, false, tag,
        reduceKeys.size(), numReds, AcidUtils.Operation.NOT_ACID);

    ReduceSinkOperator rsOp = (ReduceSinkOperator) putOpInsertMap(
        OperatorFactory.getAndMakeChild(rsDesc, new RowSchema(outputRR
            .getColumnInfos()), child), outputRR);
    List<String> keyColNames = rsDesc.getOutputKeyColumnNames();
    for (int i = 0 ; i < keyColNames.size(); i++) {
      colExprMap.put(Utilities.ReduceField.KEY + "." + keyColNames.get(i), reduceKeys.get(i));
    }
    List<String> valColNames = rsDesc.getOutputValueColumnNames();
    for (int i = 0 ; i < valColNames.size(); i++) {
      colExprMap.put(Utilities.ReduceField.VALUE + "." + valColNames.get(i), reduceValues.get(i));
    }

    rsOp.setValueIndex(index);
    rsOp.setColumnExprMap(colExprMap);
    rsOp.setInputAliases(srcs);
    return rsOp;
  }

  private Operator genJoinOperator(QB qb, QBJoinTree joinTree,
      Map<String, Operator> map,
      Operator joiningOp) throws SemanticException {
    QBJoinTree leftChild = joinTree.getJoinSrc();
    Operator joinSrcOp = joiningOp instanceof JoinOperator ? joiningOp : null;

    if (joinSrcOp == null && leftChild != null) {
      joinSrcOp = genJoinOperator(qb, leftChild, map, null);
    }

    if ( joinSrcOp != null ) {
      ArrayList<ASTNode> filter = joinTree.getFiltersForPushing().get(0);
      for (ASTNode cond : filter) {
        joinSrcOp = genFilterPlan(qb, cond, joinSrcOp);
      }
    }

    String[] baseSrc = joinTree.getBaseSrc();
    Operator[] srcOps = new Operator[baseSrc.length];

    HashSet<Integer> omitOpts = null; // set of input to the join that should be
    // omitted by the output
    int pos = 0;
    for (String src : baseSrc) {
      if (src != null) {
        Operator srcOp = map.get(src.toLowerCase());

        // for left-semi join, generate an additional selection & group-by
        // operator before ReduceSink
        ArrayList<ASTNode> fields = joinTree.getRHSSemijoinColumns(src);
        if (fields != null) {
          // the RHS table columns should be not be output from the join
          if (omitOpts == null) {
            omitOpts = new HashSet<Integer>();
          }
          omitOpts.add(pos);

          // generate a selection operator for group-by keys only
          srcOp = insertSelectForSemijoin(fields, srcOp);

          // generate a groupby operator (HASH mode) for a map-side partial
          // aggregation for semijoin
          srcOps[pos++] = genMapGroupByForSemijoin(qb, fields, srcOp,
              GroupByDesc.Mode.HASH);
        } else {
          srcOps[pos++] = srcOp;
        }
      } else {
        assert pos == 0;
        srcOps[pos++] = joinSrcOp;
      }
    }

    ExprNodeDesc[][] joinKeys = genJoinKeys(joinTree, srcOps);

    for (int i = 0; i < srcOps.length; i++) {
      // generate a ReduceSink operator for the join
      String[] srcs = baseSrc[i] != null ? new String[] {baseSrc[i]} : joinTree.getLeftAliases();
      srcOps[i] = genNotNullFilterForJoinSourcePlan(qb, srcOps[i], joinTree, joinKeys[i]);
      srcOps[i] = genJoinReduceSinkChild(qb, joinKeys[i], srcOps[i], srcs, joinTree.getNextTag());
    }

    JoinOperator joinOp = (JoinOperator) genJoinOperatorChildren(joinTree,
      joinSrcOp, srcOps, omitOpts, joinKeys);
    joinOp.getConf().setQBJoinTreeProps(joinTree);
    joinContext.put(joinOp, joinTree);

    Operator op = joinOp;
    for(ASTNode condn : joinTree.getPostJoinFilters() ) {
      op = genFilterPlan(qb, condn, op);
    }
    return op;
  }

  /**
   * Construct a selection operator for semijoin that filter out all fields
   * other than the group by keys.
   *
   * @param fields
   *          list of fields need to be output
   * @param input
   *          input operator
   * @return the selection operator.
   * @throws SemanticException
   */
  private Operator insertSelectForSemijoin(ArrayList<ASTNode> fields,
      Operator input) throws SemanticException {

    RowResolver inputRR = opParseCtx.get(input).getRowResolver();
    ArrayList<ExprNodeDesc> colList = new ArrayList<ExprNodeDesc>();
    ArrayList<String> columnNames = new ArrayList<String>();

    Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();
    // construct the list of columns that need to be projected
    for (ASTNode field : fields) {
      ExprNodeColumnDesc exprNode = (ExprNodeColumnDesc) genExprNodeDesc(field,
          inputRR);
      colList.add(exprNode);
      columnNames.add(exprNode.getColumn());
      colExprMap.put(exprNode.getColumn(), exprNode);
    }

    // create selection operator
    Operator output = putOpInsertMap(OperatorFactory.getAndMakeChild(
        new SelectDesc(colList, columnNames, false), new RowSchema(inputRR
            .getColumnInfos()), input), inputRR);

    output.setColumnExprMap(colExprMap);
    return output;
  }

  private Operator genMapGroupByForSemijoin(QB qb, ArrayList<ASTNode> fields, // the
      // ASTNode
      // of
      // the
      // join
      // key
      // "tab.col"
      Operator inputOperatorInfo, GroupByDesc.Mode mode)
      throws SemanticException {

    RowResolver groupByInputRowResolver = opParseCtx.get(inputOperatorInfo)
        .getRowResolver();
    RowResolver groupByOutputRowResolver = new RowResolver();
    ArrayList<ExprNodeDesc> groupByKeys = new ArrayList<ExprNodeDesc>();
    ArrayList<String> outputColumnNames = new ArrayList<String>();
    ArrayList<AggregationDesc> aggregations = new ArrayList<AggregationDesc>();
    Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();
    qb.getParseInfo();

    groupByOutputRowResolver.setIsExprResolver(true); // join keys should only
    // be columns but not be
    // expressions

    for (int i = 0; i < fields.size(); ++i) {
      // get the group by keys to ColumnInfo
      ASTNode colName = fields.get(i);
      ExprNodeDesc grpByExprNode = genExprNodeDesc(colName,
          groupByInputRowResolver);
      groupByKeys.add(grpByExprNode);

      // generate output column names
      String field = getColumnInternalName(i);
      outputColumnNames.add(field);
      ColumnInfo colInfo2 = new ColumnInfo(field, grpByExprNode.getTypeInfo(),
          "", false);
      groupByOutputRowResolver.putExpression(colName, colInfo2);

      // establish mapping from the output column to the input column
      colExprMap.put(field, grpByExprNode);
    }

    // Generate group-by operator
    float groupByMemoryUsage = HiveConf.getFloatVar(conf, HiveConf.ConfVars.HIVEMAPAGGRHASHMEMORY);
    float memoryThreshold = HiveConf
        .getFloatVar(conf, HiveConf.ConfVars.HIVEMAPAGGRMEMORYTHRESHOLD);
    Operator op = putOpInsertMap(OperatorFactory.getAndMakeChild(
        new GroupByDesc(mode, outputColumnNames, groupByKeys, aggregations,
            false, groupByMemoryUsage, memoryThreshold, null, false, -1, false),
        new RowSchema(groupByOutputRowResolver.getColumnInfos()),
        inputOperatorInfo), groupByOutputRowResolver);

    op.setColumnExprMap(colExprMap);
    return op;
  }

  private ExprNodeDesc[][] genJoinOperatorTypeCheck(ExprNodeDesc[][] keys)
      throws SemanticException {
    // keys[i] -> ArrayList<exprNodeDesc> for the i-th join operator key list
    int keyLength = 0;
    for (int i = 0; i < keys.length; i++) {
      if (i == 0) {
        keyLength = keys[i].length;
      } else {
        assert keyLength == keys[i].length;
      }
    }
    // implicit type conversion hierarchy
    for (int k = 0; k < keyLength; k++) {
      // Find the common class for type conversion
      TypeInfo commonType = keys[0][k].getTypeInfo();
      for (int i = 1; i < keys.length; i++) {
        TypeInfo a = commonType;
        TypeInfo b = keys[i][k].getTypeInfo();
        commonType = FunctionRegistry.getCommonClassForComparison(a, b);
        if (commonType == null) {
          throw new SemanticException(
              "Cannot do equality join on different types: " + a.getTypeName()
                  + " and " + b.getTypeName());
        }
      }
      // Add implicit type conversion if necessary
      for (int i = 0; i < keys.length; i++) {
        if (TypeInfoUtils.isConversionRequiredForComparison(
                keys[i][k].getTypeInfo(), commonType)) {
          keys[i][k] = ParseUtils.createConversionCast(
                  keys[i][k], (PrimitiveTypeInfo)commonType);
        }
      }
    }
    return keys;
  }

  private Operator genJoinPlan(QB qb, Map<String, Operator> map)
      throws SemanticException {
    QBJoinTree joinTree = qb.getQbJoinTree();
    Operator joinOp = genJoinOperator(qb, joinTree, map, null);
    return joinOp;
  }

  /**
   * Extract the filters from the join condition and push them on top of the
   * source operators. This procedure traverses the query tree recursively,
   */
  private void pushJoinFilters(QB qb, QBJoinTree joinTree,
      Map<String, Operator> map) throws SemanticException {
    pushJoinFilters(qb, joinTree, map, true);
  }

  /**
   * Extract the filters from the join condition and push them on top of the
   * source operators. This procedure traverses the query tree recursively,
   */
  private void pushJoinFilters(QB qb, QBJoinTree joinTree,
      Map<String, Operator> map,
      boolean recursively) throws SemanticException {
    if ( recursively ) {
      if (joinTree.getJoinSrc() != null) {
        pushJoinFilters(qb, joinTree.getJoinSrc(), map);
      }
    }
    ArrayList<ArrayList<ASTNode>> filters = joinTree.getFiltersForPushing();
    int pos = 0;
    for (String src : joinTree.getBaseSrc()) {
      if (src != null) {
        Operator srcOp = map.get(src);
        ArrayList<ASTNode> filter = filters.get(pos);
        for (ASTNode cond : filter) {
          srcOp = genFilterPlan(qb, cond, srcOp);
        }
        map.put(src, srcOp);
      }
      pos++;
    }
  }

  private List<String> getMapSideJoinTables(QB qb) {
    List<String> cols = new ArrayList<String>();


    ASTNode hints = qb.getParseInfo().getHints();
    for (int pos = 0; pos < hints.getChildCount(); pos++) {
      ASTNode hint = (ASTNode) hints.getChild(pos);
      if (((ASTNode) hint.getChild(0)).getToken().getType() == HiveParser.TOK_MAPJOIN) {
        // the user has specified to ignore mapjoin hint
        if (!conf.getBoolVar(HiveConf.ConfVars.HIVEIGNOREMAPJOINHINT)
            && !conf.getVar(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE).equals("tez")) {
          ASTNode hintTblNames = (ASTNode) hint.getChild(1);
          int numCh = hintTblNames.getChildCount();
          for (int tblPos = 0; tblPos < numCh; tblPos++) {
            String tblName = ((ASTNode) hintTblNames.getChild(tblPos)).getText()
                .toLowerCase();
            if (!cols.contains(tblName)) {
              cols.add(tblName);
            }
          }
        }
        else {
          queryProperties.setMapJoinRemoved(true);
        }
      }
    }

    return cols;
  }

  // The join alias is modified before being inserted for consumption by sort-merge
  // join queries. If the join is part of a sub-query the alias is modified to include
  // the sub-query alias.
  private String getModifiedAlias(QB qb, String alias) {
    return QB.getAppendedAliasFromId(qb.getId(), alias);
  }

  private QBJoinTree genUniqueJoinTree(QB qb, ASTNode joinParseTree,
      Map<String, Operator> aliasToOpInfo)
      throws SemanticException {
    QBJoinTree joinTree = new QBJoinTree();
    joinTree.setNoOuterJoin(false);

    joinTree.setExpressions(new ArrayList<ArrayList<ASTNode>>());
    joinTree.setFilters(new ArrayList<ArrayList<ASTNode>>());
    joinTree.setFiltersForPushing(new ArrayList<ArrayList<ASTNode>>());

    // Create joinTree structures to fill them up later
    ArrayList<String> rightAliases = new ArrayList<String>();
    ArrayList<String> leftAliases = new ArrayList<String>();
    ArrayList<String> baseSrc = new ArrayList<String>();
    ArrayList<Boolean> preserved = new ArrayList<Boolean>();

    boolean lastPreserved = false;
    int cols = -1;

    for (int i = 0; i < joinParseTree.getChildCount(); i++) {
      ASTNode child = (ASTNode) joinParseTree.getChild(i);

      switch (child.getToken().getType()) {
      case HiveParser.TOK_TABREF:
        // Handle a table - populate aliases appropriately:
        // leftAliases should contain the first table, rightAliases should
        // contain all other tables and baseSrc should contain all tables

        String tableName = getUnescapedUnqualifiedTableName((ASTNode) child.getChild(0));

        String alias = child.getChildCount() == 1 ? tableName
            : unescapeIdentifier(child.getChild(child.getChildCount() - 1)
                .getText().toLowerCase());

        if (i == 0) {
          leftAliases.add(alias);
          joinTree.setLeftAlias(alias);
        } else {
          rightAliases.add(alias);
        }
        joinTree.getAliasToOpInfo().put(
            getModifiedAlias(qb, alias), aliasToOpInfo.get(alias));
        joinTree.setId(qb.getId());
        baseSrc.add(alias);

        preserved.add(lastPreserved);
        lastPreserved = false;
        break;

      case HiveParser.TOK_EXPLIST:
        if (cols == -1 && child.getChildCount() != 0) {
          cols = child.getChildCount();
        } else if (child.getChildCount() != cols) {
          throw new SemanticException("Tables with different or invalid "
              + "number of keys in UNIQUEJOIN");
        }

        ArrayList<ASTNode> expressions = new ArrayList<ASTNode>();
        ArrayList<ASTNode> filt = new ArrayList<ASTNode>();
        ArrayList<ASTNode> filters = new ArrayList<ASTNode>();

        for (Node exp : child.getChildren()) {
          expressions.add((ASTNode) exp);
        }

        joinTree.getExpressions().add(expressions);
        joinTree.getFilters().add(filt);
        joinTree.getFiltersForPushing().add(filters);
        break;

      case HiveParser.KW_PRESERVE:
        lastPreserved = true;
        break;

      case HiveParser.TOK_SUBQUERY:
        throw new SemanticException(
            "Subqueries are not supported in UNIQUEJOIN");

      default:
        throw new SemanticException("Unexpected UNIQUEJOIN structure");
      }
    }

    joinTree.setBaseSrc(baseSrc.toArray(new String[0]));
    joinTree.setLeftAliases(leftAliases.toArray(new String[0]));
    joinTree.setRightAliases(rightAliases.toArray(new String[0]));

    JoinCond[] condn = new JoinCond[preserved.size()];
    for (int i = 0; i < condn.length; i++) {
      condn[i] = new JoinCond(preserved.get(i));
    }
    joinTree.setJoinCond(condn);

    if ((qb.getParseInfo().getHints() != null)
        && !(conf.getVar(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE).equals("tez"))) {
      LOG.info("STREAMTABLE hint honored.");
      parseStreamTables(joinTree, qb);
    }

    return joinTree;
  }

  /*
   * Setup a QBJoinTree between a SubQuery and its Parent Query. The Parent Query
   * is the lhs of the Join.
   *
   * The Parent Query is represented by the last Operator needed to process its From Clause.
   * In case of a single table Query this will be a TableScan, but it can be a Join Operator
   * if the Parent Query contains Join clauses, or in case of a single source from clause,
   * the source could be a SubQuery or a PTF invocation.
   *
   * We setup the QBJoinTree with the above constrains in place. So:
   * - the lhs of the QBJoinTree can be a another QBJoinTree if the Parent Query operator
   *   is a JoinOperator. In this case we get its QBJoinTree from the 'joinContext'
   * - the rhs is always a reference to the SubQuery. Its alias is obtained from the
   *   QBSubQuery object.
   *
   * The QBSubQuery also provides the Joining Condition AST. The Joining Condition has been
   * transformed in QBSubQuery setup, before this call. The Joining condition has any correlated
   * predicates and a predicate for joining the Parent Query expression with the SubQuery.
   *
   * The QBSubQuery also specifies what kind of Join to construct.
   *
   * Given this information, once we initialize the QBJoinTree, we call the 'parseJoinCondition'
   * method to validate and parse Join conditions.
   */
  private QBJoinTree genSQJoinTree(QB qb, ISubQueryJoinInfo subQuery,
      Operator joiningOp,
      Map<String, Operator> aliasToOpInfo)
          throws SemanticException {
    QBJoinTree joinTree = new QBJoinTree();
    JoinCond[] condn = new JoinCond[1];

    switch (subQuery.getJoinType()) {
    case LEFTOUTER:
      joinTree.setNoOuterJoin(false);
      condn[0] = new JoinCond(0, 1, JoinType.LEFTOUTER);
      break;
    case RIGHTOUTER:
      joinTree.setNoOuterJoin(false);
      condn[0] = new JoinCond(0, 1, JoinType.RIGHTOUTER);
      break;
    case FULLOUTER:
      joinTree.setNoOuterJoin(false);
      condn[0] = new JoinCond(0, 1, JoinType.FULLOUTER);
      break;
    case LEFTSEMI:
      joinTree.setNoSemiJoin(false);
      condn[0] = new JoinCond(0, 1, JoinType.LEFTSEMI);
      break;
    default:
      condn[0] = new JoinCond(0, 1, JoinType.INNER);
      joinTree.setNoOuterJoin(true);
      break;
    }
    joinTree.setJoinCond(condn);

    if ( joiningOp instanceof JoinOperator ) {
      QBJoinTree leftTree = joinContext.get(joiningOp);
      joinTree.setJoinSrc(leftTree);
      String[] leftChildAliases = leftTree.getLeftAliases();
      String leftAliases[] = new String[leftChildAliases.length + 1];
      for (int i = 0; i < leftChildAliases.length; i++) {
        leftAliases[i] = leftChildAliases[i];
      }
      leftAliases[leftChildAliases.length] = leftTree.getRightAliases()[0];
      joinTree.setLeftAliases(leftAliases);
    } else {
      String alias = unescapeIdentifier(
          SubQueryUtils.getAlias(joiningOp, aliasToOpInfo).toLowerCase());
      joinTree.setLeftAlias(alias);
      String[] leftAliases = new String[1];
      leftAliases[0] = alias;
      joinTree.setLeftAliases(leftAliases);
      String[] children = new String[2];
      children[0] = alias;
      joinTree.setBaseSrc(children);
      joinTree.setId(qb.getId());
      joinTree.getAliasToOpInfo().put(
          getModifiedAlias(qb, alias), aliasToOpInfo.get(alias));
    }

    String rightalias = unescapeIdentifier(subQuery.getAlias().toLowerCase());
    String[] rightAliases = new String[1];
    rightAliases[0] = rightalias;
    joinTree.setRightAliases(rightAliases);
    String[] children = joinTree.getBaseSrc();
    if (children == null) {
      children = new String[2];
    }
    children[1] = rightalias;
    joinTree.setBaseSrc(children);
    joinTree.setId(qb.getId());
    joinTree.getAliasToOpInfo().put(
        getModifiedAlias(qb, rightalias), aliasToOpInfo.get(rightalias));
    // remember rhs table for semijoin
    if (joinTree.getNoSemiJoin() == false) {
      joinTree.addRHSSemijoin(rightalias);
    }

    ArrayList<ArrayList<ASTNode>> expressions = new ArrayList<ArrayList<ASTNode>>();
    expressions.add(new ArrayList<ASTNode>());
    expressions.add(new ArrayList<ASTNode>());
    joinTree.setExpressions(expressions);

    ArrayList<Boolean> nullsafes = new ArrayList<Boolean>();
    joinTree.setNullSafes(nullsafes);

    ArrayList<ArrayList<ASTNode>> filters = new ArrayList<ArrayList<ASTNode>>();
    filters.add(new ArrayList<ASTNode>());
    filters.add(new ArrayList<ASTNode>());
    joinTree.setFilters(filters);
    joinTree.setFilterMap(new int[2][]);

    ArrayList<ArrayList<ASTNode>> filtersForPushing =
        new ArrayList<ArrayList<ASTNode>>();
    filtersForPushing.add(new ArrayList<ASTNode>());
    filtersForPushing.add(new ArrayList<ASTNode>());
    joinTree.setFiltersForPushing(filtersForPushing);

    ASTNode joinCond = subQuery.getJoinConditionAST();
    ArrayList<String> leftSrc = new ArrayList<String>();
    parseJoinCondition(joinTree, joinCond, leftSrc, aliasToOpInfo);
    if (leftSrc.size() == 1) {
      joinTree.setLeftAlias(leftSrc.get(0));
    }

    return joinTree;
  }

  private QBJoinTree genJoinTree(QB qb, ASTNode joinParseTree,
      Map<String, Operator> aliasToOpInfo)
      throws SemanticException {
    QBJoinTree joinTree = new QBJoinTree();
    JoinCond[] condn = new JoinCond[1];

    switch (joinParseTree.getToken().getType()) {
    case HiveParser.TOK_LEFTOUTERJOIN:
      joinTree.setNoOuterJoin(false);
      condn[0] = new JoinCond(0, 1, JoinType.LEFTOUTER);
      break;
    case HiveParser.TOK_RIGHTOUTERJOIN:
      joinTree.setNoOuterJoin(false);
      condn[0] = new JoinCond(0, 1, JoinType.RIGHTOUTER);
      break;
    case HiveParser.TOK_FULLOUTERJOIN:
      joinTree.setNoOuterJoin(false);
      condn[0] = new JoinCond(0, 1, JoinType.FULLOUTER);
      break;
    case HiveParser.TOK_LEFTSEMIJOIN:
      joinTree.setNoSemiJoin(false);
      condn[0] = new JoinCond(0, 1, JoinType.LEFTSEMI);
      break;
    default:
      condn[0] = new JoinCond(0, 1, JoinType.INNER);
      joinTree.setNoOuterJoin(true);
      break;
    }

    joinTree.setJoinCond(condn);

    ASTNode left = (ASTNode) joinParseTree.getChild(0);
    ASTNode right = (ASTNode) joinParseTree.getChild(1);

    if ((left.getToken().getType() == HiveParser.TOK_TABREF)
        || (left.getToken().getType() == HiveParser.TOK_SUBQUERY)
        || (left.getToken().getType() == HiveParser.TOK_PTBLFUNCTION)) {
      String tableName = getUnescapedUnqualifiedTableName((ASTNode) left.getChild(0))
          .toLowerCase();
      String alias = extractJoinAlias(left, tableName);
      joinTree.setLeftAlias(alias);
      String[] leftAliases = new String[1];
      leftAliases[0] = alias;
      joinTree.setLeftAliases(leftAliases);
      String[] children = new String[2];
      children[0] = alias;
      joinTree.setBaseSrc(children);
      joinTree.setId(qb.getId());
      joinTree.getAliasToOpInfo().put(
          getModifiedAlias(qb, alias), aliasToOpInfo.get(alias));
    } else if (isJoinToken(left)) {
      QBJoinTree leftTree = genJoinTree(qb, left, aliasToOpInfo);
      joinTree.setJoinSrc(leftTree);
      String[] leftChildAliases = leftTree.getLeftAliases();
      String leftAliases[] = new String[leftChildAliases.length + 1];
      for (int i = 0; i < leftChildAliases.length; i++) {
        leftAliases[i] = leftChildAliases[i];
      }
      leftAliases[leftChildAliases.length] = leftTree.getRightAliases()[0];
      joinTree.setLeftAliases(leftAliases);
    } else {
      assert (false);
    }

    if ((right.getToken().getType() == HiveParser.TOK_TABREF)
        || (right.getToken().getType() == HiveParser.TOK_SUBQUERY)
        || (right.getToken().getType() == HiveParser.TOK_PTBLFUNCTION)) {
      String tableName = getUnescapedUnqualifiedTableName((ASTNode) right.getChild(0))
          .toLowerCase();
      String alias = extractJoinAlias(right, tableName);
      String[] rightAliases = new String[1];
      rightAliases[0] = alias;
      joinTree.setRightAliases(rightAliases);
      String[] children = joinTree.getBaseSrc();
      if (children == null) {
        children = new String[2];
      }
      children[1] = alias;
      joinTree.setBaseSrc(children);
      joinTree.setId(qb.getId());
      joinTree.getAliasToOpInfo().put(
          getModifiedAlias(qb, alias), aliasToOpInfo.get(alias));
      // remember rhs table for semijoin
      if (joinTree.getNoSemiJoin() == false) {
        joinTree.addRHSSemijoin(alias);
      }
    } else {
      assert false;
    }

    ArrayList<ArrayList<ASTNode>> expressions = new ArrayList<ArrayList<ASTNode>>();
    expressions.add(new ArrayList<ASTNode>());
    expressions.add(new ArrayList<ASTNode>());
    joinTree.setExpressions(expressions);

    ArrayList<Boolean> nullsafes = new ArrayList<Boolean>();
    joinTree.setNullSafes(nullsafes);

    ArrayList<ArrayList<ASTNode>> filters = new ArrayList<ArrayList<ASTNode>>();
    filters.add(new ArrayList<ASTNode>());
    filters.add(new ArrayList<ASTNode>());
    joinTree.setFilters(filters);
    joinTree.setFilterMap(new int[2][]);

    ArrayList<ArrayList<ASTNode>> filtersForPushing =
        new ArrayList<ArrayList<ASTNode>>();
    filtersForPushing.add(new ArrayList<ASTNode>());
    filtersForPushing.add(new ArrayList<ASTNode>());
    joinTree.setFiltersForPushing(filtersForPushing);

    ASTNode joinCond = (ASTNode) joinParseTree.getChild(2);
    ArrayList<String> leftSrc = new ArrayList<String>();
    parseJoinCondition(joinTree, joinCond, leftSrc, aliasToOpInfo);
    if (leftSrc.size() == 1) {
      joinTree.setLeftAlias(leftSrc.get(0));
    }

    // check the hints to see if the user has specified a map-side join. This
    // will be removed later on, once the cost-based
    // infrastructure is in place
    if (qb.getParseInfo().getHints() != null) {
      List<String> mapSideTables = getMapSideJoinTables(qb);
      List<String> mapAliases = joinTree.getMapAliases();

      for (String mapTbl : mapSideTables) {
        boolean mapTable = false;
        for (String leftAlias : joinTree.getLeftAliases()) {
          if (mapTbl.equalsIgnoreCase(leftAlias)) {
            mapTable = true;
          }
        }
        for (String rightAlias : joinTree.getRightAliases()) {
          if (mapTbl.equalsIgnoreCase(rightAlias)) {
            mapTable = true;
          }
        }

        if (mapTable) {
          if (mapAliases == null) {
            mapAliases = new ArrayList<String>();
          }
          mapAliases.add(mapTbl);
          joinTree.setMapSideJoin(true);
        }
      }

      joinTree.setMapAliases(mapAliases);

      if ((conf.getVar(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE).equals("tez")) == false) {
        parseStreamTables(joinTree, qb);
      }
    }

    return joinTree;
  }

  private String extractJoinAlias(ASTNode node, String tableName) {
    // ptf node form is:
    // ^(TOK_PTBLFUNCTION $name $alias? partitionTableFunctionSource partitioningSpec? expression*)
    // guaranteed to have an alias here: check done in processJoin
    if (node.getType() == HiveParser.TOK_PTBLFUNCTION) {
      return unescapeIdentifier(node.getChild(1).getText().toLowerCase());
    }
    if (node.getChildCount() == 1) {
      return tableName;
    }
    for (int i = node.getChildCount() - 1; i >= 1; i--) {
      if (node.getChild(i).getType() == HiveParser.Identifier) {
        return unescapeIdentifier(node.getChild(i).getText().toLowerCase());
      }
    }
    return tableName;
  }

  private void parseStreamTables(QBJoinTree joinTree, QB qb) {
    List<String> streamAliases = joinTree.getStreamAliases();

    for (Node hintNode : qb.getParseInfo().getHints().getChildren()) {
      ASTNode hint = (ASTNode) hintNode;
      if (hint.getChild(0).getType() == HiveParser.TOK_STREAMTABLE) {
        for (int i = 0; i < hint.getChild(1).getChildCount(); i++) {
          if (streamAliases == null) {
            streamAliases = new ArrayList<String>();
          }
          streamAliases.add(hint.getChild(1).getChild(i).getText());
        }
      }
    }

    joinTree.setStreamAliases(streamAliases);
  }

  /**
   * Merges node to target
   */
  private void mergeJoins(QB qb, QBJoinTree node, QBJoinTree target, int pos, int[] tgtToNodeExprMap) {
    String[] nodeRightAliases = node.getRightAliases();
    String[] trgtRightAliases = target.getRightAliases();
    String[] rightAliases = new String[nodeRightAliases.length
        + trgtRightAliases.length];

    for (int i = 0; i < trgtRightAliases.length; i++) {
      rightAliases[i] = trgtRightAliases[i];
    }
    for (int i = 0; i < nodeRightAliases.length; i++) {
      rightAliases[i + trgtRightAliases.length] = nodeRightAliases[i];
    }
    target.setRightAliases(rightAliases);
    target.getAliasToOpInfo().putAll(node.getAliasToOpInfo());

    String[] nodeBaseSrc = node.getBaseSrc();
    String[] trgtBaseSrc = target.getBaseSrc();
    String[] baseSrc = new String[nodeBaseSrc.length + trgtBaseSrc.length - 1];

    for (int i = 0; i < trgtBaseSrc.length; i++) {
      baseSrc[i] = trgtBaseSrc[i];
    }
    for (int i = 1; i < nodeBaseSrc.length; i++) {
      baseSrc[i + trgtBaseSrc.length - 1] = nodeBaseSrc[i];
    }
    target.setBaseSrc(baseSrc);

    ArrayList<ArrayList<ASTNode>> expr = target.getExpressions();
    for (int i = 0; i < nodeRightAliases.length; i++) {
      List<ASTNode> nodeConds = node.getExpressions().get(i + 1);
      ArrayList<ASTNode> reordereNodeConds = new ArrayList<ASTNode>();
      for(int k=0; k < tgtToNodeExprMap.length; k++) {
        reordereNodeConds.add(nodeConds.get(tgtToNodeExprMap[k]));
      }
      expr.add(reordereNodeConds);
    }

    ArrayList<Boolean> nns = node.getNullSafes();
    ArrayList<Boolean> tns = target.getNullSafes();
    for (int i = 0; i < tns.size(); i++) {
      tns.set(i, tns.get(i) & nns.get(i)); // any of condition contains non-NS, non-NS
    }

    ArrayList<ArrayList<ASTNode>> filters = target.getFilters();
    for (int i = 0; i < nodeRightAliases.length; i++) {
      filters.add(node.getFilters().get(i + 1));
    }

    if (node.getFilters().get(0).size() != 0) {
      ArrayList<ASTNode> filterPos = filters.get(pos);
      filterPos.addAll(node.getFilters().get(0));
    }

    int[][] nmap = node.getFilterMap();
    int[][] tmap = target.getFilterMap();
    int[][] newmap = new int[tmap.length + nmap.length - 1][];

    for (int[] mapping : nmap) {
      if (mapping != null) {
        for (int i = 0; i < mapping.length; i += 2) {
          if (pos > 0 || mapping[i] > 0) {
            mapping[i] += trgtRightAliases.length;
          }
        }
      }
    }
    if (nmap[0] != null) {
      if (tmap[pos] == null) {
        tmap[pos] = nmap[0];
      } else {
        int[] appended = new int[tmap[pos].length + nmap[0].length];
        System.arraycopy(tmap[pos], 0, appended, 0, tmap[pos].length);
        System.arraycopy(nmap[0], 0, appended, tmap[pos].length, nmap[0].length);
        tmap[pos] = appended;
      }
    }
    System.arraycopy(tmap, 0, newmap, 0, tmap.length);
    System.arraycopy(nmap, 1, newmap, tmap.length, nmap.length - 1);
    target.setFilterMap(newmap);

    ArrayList<ArrayList<ASTNode>> filter = target.getFiltersForPushing();
    for (int i = 0; i < nodeRightAliases.length; i++) {
      filter.add(node.getFiltersForPushing().get(i + 1));
    }

    if (node.getFiltersForPushing().get(0).size() != 0) {
      /*
       * for each predicate:
       * - does it refer to one or many aliases
       * - if one: add it to the filterForPushing list of that alias
       * - if many: add as a filter from merging trees.
       */

      for(ASTNode nodeFilter : node.getFiltersForPushing().get(0) ) {
        int fPos = ParseUtils.checkJoinFilterRefersOneAlias(target.getBaseSrc(), nodeFilter);

        if ( fPos != - 1 ) {
          filter.get(fPos).add(nodeFilter);
        } else {
          target.addPostJoinFilter(nodeFilter);
        }
      }
     }

    if (node.getNoOuterJoin() && target.getNoOuterJoin()) {
      target.setNoOuterJoin(true);
    } else {
      target.setNoOuterJoin(false);
    }

    if (node.getNoSemiJoin() && target.getNoSemiJoin()) {
      target.setNoSemiJoin(true);
    } else {
      target.setNoSemiJoin(false);
    }

    target.mergeRHSSemijoin(node);

    JoinCond[] nodeCondns = node.getJoinCond();
    int nodeCondnsSize = nodeCondns.length;
    JoinCond[] targetCondns = target.getJoinCond();
    int targetCondnsSize = targetCondns.length;
    JoinCond[] newCondns = new JoinCond[nodeCondnsSize + targetCondnsSize];
    for (int i = 0; i < targetCondnsSize; i++) {
      newCondns[i] = targetCondns[i];
    }

    for (int i = 0; i < nodeCondnsSize; i++) {
      JoinCond nodeCondn = nodeCondns[i];
      if (nodeCondn.getLeft() == 0) {
        nodeCondn.setLeft(pos);
      } else {
        nodeCondn.setLeft(nodeCondn.getLeft() + targetCondnsSize);
      }
      nodeCondn.setRight(nodeCondn.getRight() + targetCondnsSize);
      newCondns[targetCondnsSize + i] = nodeCondn;
    }

    target.setJoinCond(newCondns);
    if (target.isMapSideJoin()) {
      assert node.isMapSideJoin();
      List<String> mapAliases = target.getMapAliases();
      for (String mapTbl : node.getMapAliases()) {
        if (!mapAliases.contains(mapTbl)) {
          mapAliases.add(mapTbl);
        }
      }
      target.setMapAliases(mapAliases);
    }
  }

  private ObjectPair<Integer, int[]> findMergePos(QBJoinTree node, QBJoinTree target) {
    int res = -1;
    String leftAlias = node.getLeftAlias();
    if (leftAlias == null) {
      return new ObjectPair(-1, null);
    }

    ArrayList<ASTNode> nodeCondn = node.getExpressions().get(0);
    ArrayList<ASTNode> targetCondn = null;

    if (leftAlias.equals(target.getLeftAlias())) {
      targetCondn = target.getExpressions().get(0);
      res = 0;
    } else {
      for (int i = 0; i < target.getRightAliases().length; i++) {
        if (leftAlias.equals(target.getRightAliases()[i])) {
          targetCondn = target.getExpressions().get(i + 1);
          res = i + 1;
          break;
        }
      }
    }

    if ( targetCondn == null || (nodeCondn.size() != targetCondn.size())) {
      return new ObjectPair(-1, null);
    }

    /*
     * The order of the join condition expressions don't matter.
     * A merge can happen:
     * - if every target condition is present in some position of the node condition list.
     * - there is no node condition, which is not equal to any target condition.
     */

    int[] tgtToNodeExprMap = new int[targetCondn.size()];
    boolean[] nodeFiltersMapped = new boolean[nodeCondn.size()];
    int i, j;
    for(i=0; i<targetCondn.size(); i++) {
      String tgtExprTree = targetCondn.get(i).toStringTree();
      tgtToNodeExprMap[i] = -1;
      for(j=0; j < nodeCondn.size(); j++) {
        if ( nodeCondn.get(j).toStringTree().equals(tgtExprTree)) {
          tgtToNodeExprMap[i] = j;
          nodeFiltersMapped[j] = true;
        }
      }
      if ( tgtToNodeExprMap[i] == -1) {
        return new ObjectPair(-1, null);
      }
    }

    for(j=0; j < nodeCondn.size(); j++) {
      if ( !nodeFiltersMapped[j]) {
        return new ObjectPair(-1, null);
      }
    }

    return new ObjectPair(res, tgtToNodeExprMap);
  }

  boolean continueJoinMerge() {
    return true;
  }

  // try merge join tree from inner most source
  // (it was merged from outer most to inner, which could be invalid)
  //
  // in a join tree ((A-B)-C)-D where C is not mergeable with A-B,
  // D can be merged with A-B into single join If and only if C and D has same join type
  // In this case, A-B-D join will be executed first and ABD-C join will be executed in next
  private void mergeJoinTree(QB qb) {
    QBJoinTree tree = qb.getQbJoinTree();
    if (tree.getJoinSrc() == null) {
      return;
    }
    // make array with QBJoinTree : outer most(0) --> inner most(n)
    List<QBJoinTree> trees = new ArrayList<QBJoinTree>();
    for (;tree != null; tree = tree.getJoinSrc()) {
      trees.add(tree);
    }
    // merging from 'target'(inner) to 'node'(outer)
    for (int i = trees.size() - 1; i >= 0; i--) {
      QBJoinTree target = trees.get(i);
      if (target == null) {
        continue;
      }
      JoinType prevType = null;   // save join type
      boolean continueScanning = true;
      for (int j = i - 1; j >= 0 && continueScanning; j--) {
        QBJoinTree node = trees.get(j);
        if (node == null) {
          continue;
        }
        JoinType currType = getType(node.getJoinCond());
        if (prevType != null && prevType != currType) {
          break;
        }
        ObjectPair<Integer, int[]> mergeDetails = findMergePos(node, target);
        int pos = mergeDetails.getFirst();
        if (pos >= 0) {
          // for outer joins, it should not exceed 16 aliases (short type)
          if (!node.getNoOuterJoin() || !target.getNoOuterJoin()) {
            if (node.getRightAliases().length + target.getRightAliases().length + 1 > 16) {
              LOG.info(ErrorMsg.JOINNODE_OUTERJOIN_MORETHAN_16);
              continueScanning = continueJoinMerge();
              continue;
            }
          }
          mergeJoins(qb, node, target, pos, mergeDetails.getSecond());
          trees.set(j, null);
          continue; // continue merging with next alias
        }
        /*
         * for CBO provided orderings, don't attempt to reorder joins.
         * only convert consecutive joins into n-way joins.
         */
        continueScanning = continueJoinMerge();
        if (prevType == null) {
          prevType = currType;
        }
      }
    }
    // reconstruct join tree
    QBJoinTree current = null;
    for (int i = 0; i < trees.size(); i++) {
      QBJoinTree target = trees.get(i);
      if (target == null) {
        continue;
      }
      if (current == null) {
        qb.setQbJoinTree(current = target);
      } else {
        current.setJoinSrc(target);
        current = target;
      }
    }
  }

  // Join types should be all the same for merging (or returns null)
  private JoinType getType(JoinCond[] conds) {
    JoinType type = conds[0].getJoinType();
    for (int k = 1; k < conds.length; k++) {
      if (type != conds[k].getJoinType()) {
        return null;
      }
    }
    return type;
  }

  private Operator genSelectAllDesc(Operator input) throws SemanticException {
    OpParseContext inputCtx = opParseCtx.get(input);
    RowResolver inputRR = inputCtx.getRowResolver();
    ArrayList<ColumnInfo> columns = inputRR.getColumnInfos();
    ArrayList<ExprNodeDesc> colList = new ArrayList<ExprNodeDesc>();
    ArrayList<String> columnNames = new ArrayList<String>();
    Map<String, ExprNodeDesc> columnExprMap =
        new HashMap<String, ExprNodeDesc>();
    for (int i = 0; i < columns.size(); i++) {
      ColumnInfo col = columns.get(i);
      colList.add(new ExprNodeColumnDesc(col));
      columnNames.add(col.getInternalName());
      columnExprMap.put(col.getInternalName(), new ExprNodeColumnDesc(col));
    }
    RowResolver outputRR = inputRR.duplicate();
    Operator output = putOpInsertMap(OperatorFactory.getAndMakeChild(
        new SelectDesc(colList, columnNames, true), 
        outputRR.getRowSchema(), input), outputRR);
    output.setColumnExprMap(columnExprMap);
    return output;
  }

  // Return the common distinct expression
  // There should be more than 1 destination, with group bys in all of them.
  private List<ASTNode> getCommonDistinctExprs(QB qb, Operator input) {
    QBParseInfo qbp = qb.getParseInfo();
    // If a grouping set aggregation is present, common processing is not possible
    if (!qbp.getDestCubes().isEmpty() || !qbp.getDestRollups().isEmpty()
        || !qbp.getDestToLateralView().isEmpty()) {
      return null;
    }

    RowResolver inputRR = opParseCtx.get(input).getRowResolver();
    TreeSet<String> ks = new TreeSet<String>();
    ks.addAll(qbp.getClauseNames());

    // Go over all the destination tables
    if (ks.size() <= 1) {
      return null;
    }

    List<ExprNodeDesc> oldList = null;
    List<ASTNode> oldASTList = null;

    for (String dest : ks) {
      // If a filter is present, common processing is not possible
      if (qbp.getWhrForClause(dest) != null) {
        return null;
      }

      if (qbp.getAggregationExprsForClause(dest).size() == 0
          && getGroupByForClause(qbp, dest).size() == 0) {
        return null;
      }

      // All distinct expressions must be the same
      List<ASTNode> list = qbp.getDistinctFuncExprsForClause(dest);
      if (list.isEmpty()) {
        return null;
      }

      List<ExprNodeDesc> currDestList;
      try {
        currDestList = getDistinctExprs(qbp, dest, inputRR);
      } catch (SemanticException e) {
        return null;
      }

      List<ASTNode> currASTList = new ArrayList<ASTNode>();
      for (ASTNode value : list) {
        // 0 is function name
        for (int i = 1; i < value.getChildCount(); i++) {
          ASTNode parameter = (ASTNode) value.getChild(i);
          currASTList.add(parameter);
        }
        if (oldList == null) {
          oldList = currDestList;
          oldASTList = currASTList;
        } else {
          if (!matchExprLists(oldList, currDestList)) {
            return null;
          }
        }
      }
    }

    return oldASTList;
  }

  private Operator createCommonReduceSink(QB qb, Operator input)
      throws SemanticException {
    // Go over all the tables and extract the common distinct key
    List<ASTNode> distExprs = getCommonDistinctExprs(qb, input);

    QBParseInfo qbp = qb.getParseInfo();
    TreeSet<String> ks = new TreeSet<String>();
    ks.addAll(qbp.getClauseNames());

    // Pass the entire row
    RowResolver inputRR = opParseCtx.get(input).getRowResolver();
    RowResolver reduceSinkOutputRowResolver = new RowResolver();
    reduceSinkOutputRowResolver.setIsExprResolver(true);
    ArrayList<ExprNodeDesc> reduceKeys = new ArrayList<ExprNodeDesc>();
    ArrayList<ExprNodeDesc> reduceValues = new ArrayList<ExprNodeDesc>();
    Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();

    // Pre-compute distinct group-by keys and store in reduceKeys

    List<String> outputColumnNames = new ArrayList<String>();
    for (ASTNode distn : distExprs) {
      ExprNodeDesc distExpr = genExprNodeDesc(distn, inputRR);
      if (reduceSinkOutputRowResolver.getExpression(distn) == null) {
        reduceKeys.add(distExpr);
        outputColumnNames.add(getColumnInternalName(reduceKeys.size() - 1));
        String field = Utilities.ReduceField.KEY.toString() + "."
            + getColumnInternalName(reduceKeys.size() - 1);
        ColumnInfo colInfo = new ColumnInfo(field, reduceKeys.get(
            reduceKeys.size() - 1).getTypeInfo(), "", false);
        reduceSinkOutputRowResolver.putExpression(distn, colInfo);
        colExprMap.put(colInfo.getInternalName(), distExpr);
      }
    }

    // Go over all the grouping keys and aggregations
    for (String dest : ks) {

      List<ASTNode> grpByExprs = getGroupByForClause(qbp, dest);
      for (int i = 0; i < grpByExprs.size(); ++i) {
        ASTNode grpbyExpr = grpByExprs.get(i);

        if (reduceSinkOutputRowResolver.getExpression(grpbyExpr) == null) {
          ExprNodeDesc grpByExprNode = genExprNodeDesc(grpbyExpr, inputRR);
          reduceValues.add(grpByExprNode);
          String field = Utilities.ReduceField.VALUE.toString() + "."
              + getColumnInternalName(reduceValues.size() - 1);
          ColumnInfo colInfo = new ColumnInfo(field, reduceValues.get(
              reduceValues.size() - 1).getTypeInfo(), "", false);
          reduceSinkOutputRowResolver.putExpression(grpbyExpr, colInfo);
          outputColumnNames.add(getColumnInternalName(reduceValues.size() - 1));
          colExprMap.put(field, grpByExprNode);
        }
      }

      // For each aggregation
      HashMap<String, ASTNode> aggregationTrees = qbp
          .getAggregationExprsForClause(dest);
      assert (aggregationTrees != null);

      for (Map.Entry<String, ASTNode> entry : aggregationTrees.entrySet()) {
        ASTNode value = entry.getValue();

        // 0 is the function name
        for (int i = 1; i < value.getChildCount(); i++) {
          ASTNode paraExpr = (ASTNode) value.getChild(i);

          if (reduceSinkOutputRowResolver.getExpression(paraExpr) == null) {
            ExprNodeDesc paraExprNode = genExprNodeDesc(paraExpr, inputRR);
            reduceValues.add(paraExprNode);
            String field = Utilities.ReduceField.VALUE.toString() + "."
                + getColumnInternalName(reduceValues.size() - 1);
            ColumnInfo colInfo = new ColumnInfo(field, reduceValues.get(
                reduceValues.size() - 1).getTypeInfo(), "", false);
            reduceSinkOutputRowResolver.putExpression(paraExpr, colInfo);
            outputColumnNames
                .add(getColumnInternalName(reduceValues.size() - 1));
            colExprMap.put(field, paraExprNode);
          }
        }
      }
    }

    ReduceSinkOperator rsOp = (ReduceSinkOperator) putOpInsertMap(
        OperatorFactory.getAndMakeChild(PlanUtils.getReduceSinkDesc(reduceKeys,
            reduceValues, outputColumnNames, true, -1, reduceKeys.size(), -1,
                AcidUtils.Operation.NOT_ACID),
            new RowSchema(reduceSinkOutputRowResolver.getColumnInfos()), input),
        reduceSinkOutputRowResolver);

    rsOp.setColumnExprMap(colExprMap);
    return rsOp;
  }

  // Groups the clause names into lists so that any two clauses in the same list has the same
  // group by and distinct keys and no clause appears in more than one list. Returns a list of the
  // lists of clauses.
  private List<List<String>> getCommonGroupByDestGroups(QB qb,
      Map<String, Operator<? extends OperatorDesc>> inputs) throws SemanticException {

    QBParseInfo qbp = qb.getParseInfo();

    TreeSet<String> ks = new TreeSet<String>();
    ks.addAll(qbp.getClauseNames());

    List<List<String>> commonGroupByDestGroups = new ArrayList<List<String>>();

    // If this is a trivial query block return
    if (ks.size() <= 1) {
      List<String> oneList = new ArrayList<String>(1);
      if (ks.size() == 1) {
        oneList.add(ks.first());
      }
      commonGroupByDestGroups.add(oneList);
      return commonGroupByDestGroups;
    }

    List<Operator<? extends OperatorDesc>> inputOperators =
        new ArrayList<Operator<? extends OperatorDesc>>(ks.size());
    List<List<ExprNodeDesc>> sprayKeyLists = new ArrayList<List<ExprNodeDesc>>(ks.size());
    List<List<ExprNodeDesc>> distinctKeyLists = new ArrayList<List<ExprNodeDesc>>(ks.size());

    // Iterate over each clause
    for (String dest : ks) {
      Operator input = inputs.get(dest);
      RowResolver inputRR = opParseCtx.get(input).getRowResolver();

      List<ExprNodeDesc> distinctKeys = getDistinctExprs(qbp, dest, inputRR);
      List<ExprNodeDesc> sprayKeys = new ArrayList<ExprNodeDesc>();

      // Add the group by expressions
      List<ASTNode> grpByExprs = getGroupByForClause(qbp, dest);
      for (ASTNode grpByExpr : grpByExprs) {
        ExprNodeDesc exprDesc = genExprNodeDesc(grpByExpr, inputRR);
        if (ExprNodeDescUtils.indexOf(exprDesc, sprayKeys) < 0) {
          sprayKeys.add(exprDesc);
        }
      }

      // Loop through each of the lists of exprs, looking for a match
      boolean found = false;
      for (int i = 0; i < sprayKeyLists.size(); i++) {
        if (!input.equals(inputOperators.get(i))) {
          continue;
        }

        if (distinctKeys.isEmpty()) {
          // current dest has no distinct keys.
          List<ExprNodeDesc> combinedList = new ArrayList<ExprNodeDesc>();
          combineExprNodeLists(sprayKeyLists.get(i), distinctKeyLists.get(i), combinedList);
          if (!matchExprLists(combinedList, sprayKeys)) {
            continue;
          } // else do the common code at the end.
        } else {
          if (distinctKeyLists.get(i).isEmpty()) {
            List<ExprNodeDesc> combinedList = new ArrayList<ExprNodeDesc>();
            combineExprNodeLists(sprayKeys, distinctKeys, combinedList);
            if (!matchExprLists(combinedList, sprayKeyLists.get(i))) {
              continue;
            } else {
              // we have found a match. insert this distinct clause to head.
              distinctKeyLists.remove(i);
              sprayKeyLists.remove(i);
              distinctKeyLists.add(i, distinctKeys);
              sprayKeyLists.add(i, sprayKeys);
              commonGroupByDestGroups.get(i).add(0, dest);
              found = true;
              break;
            }
          } else {
            if (!matchExprLists(distinctKeyLists.get(i), distinctKeys)) {
              continue;
            }

            if (!matchExprLists(sprayKeyLists.get(i), sprayKeys)) {
              continue;
            }
            // else do common code
          }
        }

        // common code
        // A match was found, so add the clause to the corresponding list
        commonGroupByDestGroups.get(i).add(dest);
        found = true;
        break;
      }

      // No match was found, so create new entries
      if (!found) {
        inputOperators.add(input);
        sprayKeyLists.add(sprayKeys);
        distinctKeyLists.add(distinctKeys);
        List<String> destGroup = new ArrayList<String>();
        destGroup.add(dest);
        commonGroupByDestGroups.add(destGroup);
      }
    }

    return commonGroupByDestGroups;
  }

  private void combineExprNodeLists(List<ExprNodeDesc> list, List<ExprNodeDesc> list2,
      List<ExprNodeDesc> combinedList) {
    combinedList.addAll(list);
    for (ExprNodeDesc elem : list2) {
      if (!combinedList.contains(elem)) {
        combinedList.add(elem);
      }
    }
  }

  // Returns whether or not two lists contain the same elements independent of order
  private boolean matchExprLists(List<ExprNodeDesc> list1, List<ExprNodeDesc> list2) {

    if (list1.size() != list2.size()) {
      return false;
    }
    for (ExprNodeDesc exprNodeDesc : list1) {
      if (ExprNodeDescUtils.indexOf(exprNodeDesc, list2) < 0) {
        return false;
      }
    }

    return true;
  }

  // Returns a list of the distinct exprs without duplicates for a given clause name
  private List<ExprNodeDesc> getDistinctExprs(QBParseInfo qbp, String dest, RowResolver inputRR)
      throws SemanticException {

    List<ASTNode> distinctAggExprs = qbp.getDistinctFuncExprsForClause(dest);
    List<ExprNodeDesc> distinctExprs = new ArrayList<ExprNodeDesc>();

    for (ASTNode distinctAggExpr : distinctAggExprs) {
      // 0 is function name
      for (int i = 1; i < distinctAggExpr.getChildCount(); i++) {
        ASTNode parameter = (ASTNode) distinctAggExpr.getChild(i);
        ExprNodeDesc expr = genExprNodeDesc(parameter, inputRR);
        if (ExprNodeDescUtils.indexOf(expr, distinctExprs) < 0) {
          distinctExprs.add(expr);
        }
      }
    }

    return distinctExprs;
  }

  // see if there are any distinct expressions
  private boolean distinctExprsExists(QB qb) {
    QBParseInfo qbp = qb.getParseInfo();

    TreeSet<String> ks = new TreeSet<String>();
    ks.addAll(qbp.getClauseNames());

    for (String dest : ks) {
      List<ASTNode> list = qbp.getDistinctFuncExprsForClause(dest);
      if (!list.isEmpty()) {
        return true;
      }
    }
    return false;
  }

  @SuppressWarnings("nls")
  private Operator genBodyPlan(QB qb, Operator input, Map<String, Operator> aliasToOpInfo)
      throws SemanticException {
    QBParseInfo qbp = qb.getParseInfo();

    TreeSet<String> ks = new TreeSet<String>(qbp.getClauseNames());
    Map<String, Operator<? extends OperatorDesc>> inputs = createInputForDests(qb, input, ks);
    // For multi-group by with the same distinct, we ignore all user hints
    // currently. It doesnt matter whether he has asked to do
    // map-side aggregation or not. Map side aggregation is turned off
    List<ASTNode> commonDistinctExprs = getCommonDistinctExprs(qb, input);

    // Consider a query like:
    //
    //  from src
    //    insert overwrite table dest1 select col1, count(distinct colx) group by col1
    //    insert overwrite table dest2 select col2, count(distinct colx) group by col2;
    //
    // With HIVE_OPTIMIZE_MULTI_GROUPBY_COMMON_DISTINCTS set to true, first we spray by the distinct
    // value (colx), and then perform the 2 groups bys. This makes sense if map-side aggregation is
    // turned off. However, with maps-side aggregation, it might be useful in some cases to treat
    // the 2 inserts independently, thereby performing the query above in 2MR jobs instead of 3
    // (due to spraying by distinct key first).
    boolean optimizeMultiGroupBy = commonDistinctExprs != null &&
        conf.getBoolVar(HiveConf.ConfVars.HIVE_OPTIMIZE_MULTI_GROUPBY_COMMON_DISTINCTS);

    Operator curr = input;

    // if there is a single distinct, optimize that. Spray initially by the
    // distinct key,
    // no computation at the mapper. Have multiple group by operators at the
    // reducer - and then
    // proceed
    if (optimizeMultiGroupBy) {
      curr = createCommonReduceSink(qb, input);

      RowResolver currRR = opParseCtx.get(curr).getRowResolver();
      // create a forward operator
      input = putOpInsertMap(OperatorFactory.getAndMakeChild(new ForwardDesc(),
          new RowSchema(currRR.getColumnInfos()), curr), currRR);

      for (String dest : ks) {
        curr = input;
        curr = genGroupByPlan2MRMultiGroupBy(dest, qb, curr);
        curr = genSelectPlan(dest, qb, curr, null); // TODO: we may need to pass "input" here instead of null
        Integer limit = qbp.getDestLimit(dest);
        if (limit != null) {
          curr = genLimitMapRedPlan(dest, qb, curr, limit.intValue(), true);
          qb.getParseInfo().setOuterQueryLimit(limit.intValue());
        }
        curr = genFileSinkPlan(dest, qb, curr);
      }
    } else {
      List<List<String>> commonGroupByDestGroups = null;

      // If we can put multiple group bys in a single reducer, determine suitable groups of
      // expressions, otherwise treat all the expressions as a single group
      if (conf.getBoolVar(HiveConf.ConfVars.HIVEMULTIGROUPBYSINGLEREDUCER)) {
        try {
          commonGroupByDestGroups = getCommonGroupByDestGroups(qb, inputs);
        } catch (SemanticException e) {
          LOG.error("Failed to group clauses by common spray keys.", e);
        }
      }

      if (commonGroupByDestGroups == null) {
        commonGroupByDestGroups = new ArrayList<List<String>>();
        commonGroupByDestGroups.add(new ArrayList<String>(ks));
      }

      if (!commonGroupByDestGroups.isEmpty()) {

        // Iterate over each group of subqueries with the same group by/distinct keys
        for (List<String> commonGroupByDestGroup : commonGroupByDestGroups) {
          if (commonGroupByDestGroup.isEmpty()) {
            continue;
          }

          String firstDest = commonGroupByDestGroup.get(0);
          input = inputs.get(firstDest);

          // Constructs a standard group by plan if:
          // There is no other subquery with the same group by/distinct keys or
          // (There are no aggregations in a representative query for the group and
          // There is no group by in that representative query) or
          // The data is skewed or
          // The conf variable used to control combining group bys into a single reducer is false
          if (commonGroupByDestGroup.size() == 1 ||
              (qbp.getAggregationExprsForClause(firstDest).size() == 0 &&
              getGroupByForClause(qbp, firstDest).size() == 0) ||
              conf.getBoolVar(HiveConf.ConfVars.HIVEGROUPBYSKEW) ||
              !conf.getBoolVar(HiveConf.ConfVars.HIVEMULTIGROUPBYSINGLEREDUCER)) {

            // Go over all the destination tables
            for (String dest : commonGroupByDestGroup) {
              curr = inputs.get(dest);

              if (qbp.getWhrForClause(dest) != null) {
                ASTNode whereExpr = qb.getParseInfo().getWhrForClause(dest);
                curr = genFilterPlan((ASTNode) whereExpr.getChild(0), qb, curr, aliasToOpInfo, false);
              }
              // Preserve operator before the GBY - we'll use it to resolve '*'
              Operator<?> gbySource = curr;

              if (qbp.getAggregationExprsForClause(dest).size() != 0
                  || getGroupByForClause(qbp, dest).size() > 0) {
                // multiple distincts is not supported with skew in data
                if (conf.getBoolVar(HiveConf.ConfVars.HIVEGROUPBYSKEW) &&
                    qbp.getDistinctFuncExprsForClause(dest).size() > 1) {
                  throw new SemanticException(ErrorMsg.UNSUPPORTED_MULTIPLE_DISTINCTS.
                      getMsg());
                }
                // insert a select operator here used by the ColumnPruner to reduce
                // the data to shuffle
                curr = genSelectAllDesc(curr);
                // Check and transform group by *. This will only happen for select distinct *.
                // Here the "genSelectPlan" is being leveraged.
                // The main benefits are (1) remove virtual columns that should
                // not be included in the group by; (2) add the fully qualified column names to unParseTranslator
                // so that view is supported. The drawback is that an additional SEL op is added. If it is
                // not necessary, it will be removed by NonBlockingOpDeDupProc Optimizer because it will match
                // SEL%SEL% rule.
                ASTNode selExprList = qbp.getSelForClause(dest);
                if (selExprList.getToken().getType() == HiveParser.TOK_SELECTDI
                    && selExprList.getChildCount() == 1 && selExprList.getChild(0).getChildCount() == 1) {
                  ASTNode node = (ASTNode) selExprList.getChild(0).getChild(0);
                  if (node.getToken().getType() == HiveParser.TOK_ALLCOLREF) {
                    curr = genSelectPlan(dest, qb, curr, curr);
                    RowResolver rr = opParseCtx.get(curr).getRowResolver();
                    qbp.setSelExprForClause(dest, SemanticAnalyzer.genSelectDIAST(rr));
                  }
                }
                if (conf.getBoolVar(HiveConf.ConfVars.HIVEMAPSIDEAGGREGATE)) {
                  if (!conf.getBoolVar(HiveConf.ConfVars.HIVEGROUPBYSKEW)) {
                    curr = genGroupByPlanMapAggrNoSkew(dest, qb, curr);
                  } else {
                    curr = genGroupByPlanMapAggr2MR(dest, qb, curr);
                  }
                } else if (conf.getBoolVar(HiveConf.ConfVars.HIVEGROUPBYSKEW)) {
                  curr = genGroupByPlan2MR(dest, qb, curr);
                } else {
                  curr = genGroupByPlan1MR(dest, qb, curr);
                }
              }
              if (LOG.isDebugEnabled()) {
                LOG.debug("RR before GB " + opParseCtx.get(gbySource).getRowResolver()
                    + " after GB " + opParseCtx.get(curr).getRowResolver());
              }

              curr = genPostGroupByBodyPlan(curr, dest, qb, aliasToOpInfo, gbySource);
            }
          } else {
            curr = genGroupByPlan1ReduceMultiGBY(commonGroupByDestGroup, qb, input, aliasToOpInfo);
          }
        }
      }
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Created Body Plan for Query Block " + qb.getId());
    }

    return curr;
  }

  private Map<String, Operator<? extends OperatorDesc>> createInputForDests(QB qb,
      Operator<? extends OperatorDesc> input, Set<String> dests) throws SemanticException {
    Map<String, Operator<? extends OperatorDesc>> inputs =
        new HashMap<String, Operator<? extends OperatorDesc>>();
    for (String dest : dests) {
      inputs.put(dest, genLateralViewPlanForDest(dest, qb, input));
    }
    return inputs;
  }

  private Operator genPostGroupByBodyPlan(Operator curr, String dest, QB qb,
      Map<String, Operator> aliasToOpInfo, Operator gbySource)
      throws SemanticException {

    QBParseInfo qbp = qb.getParseInfo();

    // Insert HAVING plan here
    if (qbp.getHavingForClause(dest) != null) {
      if (getGroupByForClause(qbp, dest).size() == 0) {
        throw new SemanticException("HAVING specified without GROUP BY");
      }
      curr = genHavingPlan(dest, qb, curr, aliasToOpInfo);
    }


    if(queryProperties.hasWindowing() && qb.getWindowingSpec(dest) != null) {
      curr = genWindowingPlan(qb.getWindowingSpec(dest), curr);
    }

    curr = genSelectPlan(dest, qb, curr, gbySource);
    Integer limit = qbp.getDestLimit(dest);

    // Expressions are not supported currently without a alias.

    // Reduce sink is needed if the query contains a cluster by, distribute by,
    // order by or a sort by clause.
    boolean genReduceSink = false;
    boolean hasOrderBy = false;

    // Currently, expressions are not allowed in cluster by, distribute by,
    // order by or a sort by clause. For each of the above clause types, check
    // if the clause contains any expression.
    if (qbp.getClusterByForClause(dest) != null) {
      genReduceSink = true;
    }

    if (qbp.getDistributeByForClause(dest) != null) {
      genReduceSink = true;
    }

    if (qbp.getOrderByForClause(dest) != null) {
      genReduceSink = true;
      hasOrderBy = true;
    }

    if (qbp.getSortByForClause(dest) != null) {
      genReduceSink = true;
    }

    if (genReduceSink) {
      int numReducers = -1;

      // Use only 1 reducer if order by is present
      if (hasOrderBy) {
        numReducers = 1;
      }

      curr = genReduceSinkPlan(dest, qb, curr, numReducers, hasOrderBy);
    }


    if (qbp.getIsSubQ()) {
      if (limit != null) {
        // In case of order by, only 1 reducer is used, so no need of
        // another shuffle
        curr = genLimitMapRedPlan(dest, qb, curr, limit.intValue(), !hasOrderBy);
      }
    } else {
      // exact limit can be taken care of by the fetch operator
      if (limit != null) {
        boolean extraMRStep = true;

        if (hasOrderBy ||
            qb.getIsQuery() && qbp.getClusterByForClause(dest) == null &&
            qbp.getSortByForClause(dest) == null) {
          extraMRStep = false;
        }

        curr = genLimitMapRedPlan(dest, qb, curr, limit.intValue(),
            extraMRStep);
        qb.getParseInfo().setOuterQueryLimit(limit.intValue());
      }
      if (!SessionState.get().getHiveOperation().equals(HiveOperation.CREATEVIEW)) {
        curr = genFileSinkPlan(dest, qb, curr);
      }
    }

    return curr;
  }

  @SuppressWarnings("nls")
  private Operator genUnionPlan(String unionalias, String leftalias,
      Operator leftOp, String rightalias, Operator rightOp)
      throws SemanticException {

    // Currently, the unions are not merged - each union has only 2 parents. So,
    // a n-way union will lead to (n-1) union operators.
    // This can be easily merged into 1 union
    RowResolver leftRR = opParseCtx.get(leftOp).getRowResolver();
    RowResolver rightRR = opParseCtx.get(rightOp).getRowResolver();
    HashMap<String, ColumnInfo> leftmap = leftRR.getFieldMap(leftalias);
    HashMap<String, ColumnInfo> rightmap = rightRR.getFieldMap(rightalias);
    // make sure the schemas of both sides are the same
    ASTNode tabref = qb.getAliases().isEmpty() ? null :
        qb.getParseInfo().getSrcForAlias(qb.getAliases().get(0));
    if (leftmap.size() != rightmap.size()) {
      throw new SemanticException("Schema of both sides of union should match.");
    }
    for (Map.Entry<String, ColumnInfo> lEntry : leftmap.entrySet()) {
      String field = lEntry.getKey();
      ColumnInfo lInfo = lEntry.getValue();
      ColumnInfo rInfo = rightmap.get(field);
      if (rInfo == null) {
        throw new SemanticException(generateErrorMessage(tabref,
            "Schema of both sides of union should match. " + rightalias
                + " does not have the field " + field));
      }
      if (lInfo == null) {
        throw new SemanticException(generateErrorMessage(tabref,
            "Schema of both sides of union should match. " + leftalias
                + " does not have the field " + field));
      }
      if (!lInfo.getInternalName().equals(rInfo.getInternalName())) {
        throw new SemanticException(generateErrorMessage(tabref,
            "Schema of both sides of union should match: field " + field + ":"
                + " appears on the left side of the UNION at column position: " +
                getPositionFromInternalName(lInfo.getInternalName())
                + ", and on the right side of the UNION at column position: " +
                getPositionFromInternalName(rInfo.getInternalName())
                + ". Column positions should match for a UNION"));
      }
      // try widening coversion, otherwise fail union
      TypeInfo commonTypeInfo = FunctionRegistry.getCommonClassForUnionAll(lInfo.getType(),
          rInfo.getType());
      if (commonTypeInfo == null) {
        throw new SemanticException(generateErrorMessage(tabref,
            "Schema of both sides of union should match: Column " + field
                + " is of type " + lInfo.getType().getTypeName()
                + " on first table and type " + rInfo.getType().getTypeName()
                + " on second table"));
      }
    }

    // construct the forward operator
    RowResolver unionoutRR = new RowResolver();
    for (Map.Entry<String, ColumnInfo> lEntry : leftmap.entrySet()) {
      String field = lEntry.getKey();
      ColumnInfo lInfo = lEntry.getValue();
      ColumnInfo rInfo = rightmap.get(field);
      ColumnInfo unionColInfo = new ColumnInfo(lInfo);
      unionColInfo.setType(FunctionRegistry.getCommonClassForUnionAll(lInfo.getType(),
          rInfo.getType()));
      unionoutRR.put(unionalias, field, unionColInfo);
    }

    // For Spark, we rely on the generated SelectOperator to do the type casting.
    // Consider:
    //    SEL_1 (int)   SEL_2 (int)    SEL_3 (double)
    // If we first merge SEL_1 and SEL_2 into a UNION_1, and then merge UNION_1
    // with SEL_3 to get UNION_2, then no SelectOperator will be inserted. Hence error
    // will happen afterwards. The solution here is to insert one after UNION_1, which
    // cast int to double.
    boolean isSpark = HiveConf.getVar(conf,
        HiveConf.ConfVars.HIVE_EXECUTION_ENGINE).equals("spark");

    if (isSpark || !(leftOp instanceof UnionOperator)) {
      leftOp = genInputSelectForUnion(leftOp, leftmap, leftalias, unionoutRR, unionalias);
    }

    if (isSpark || !(rightOp instanceof UnionOperator)) {
      rightOp = genInputSelectForUnion(rightOp, rightmap, rightalias, unionoutRR, unionalias);
    }

    // If one of the children (left or right) is:
    // (i) a union, or
    // (ii) an identity projection followed by a union,
    // merge with it
    // else create a new one
    if (leftOp instanceof UnionOperator ||
        (leftOp instanceof SelectOperator &&
         leftOp.getParentOperators() != null &&
         !leftOp.getParentOperators().isEmpty() &&
         leftOp.getParentOperators().get(0) instanceof UnionOperator &&
         ((SelectOperator)leftOp).isIdentitySelect()) ) {

      if(!(leftOp instanceof UnionOperator)) {
        Operator oldChild = leftOp;
        leftOp = (Operator) leftOp.getParentOperators().get(0);
        leftOp.removeChildAndAdoptItsChildren(oldChild);
      }

      // make left a child of right
      List<Operator<? extends OperatorDesc>> child =
          new ArrayList<Operator<? extends OperatorDesc>>();
      child.add(leftOp);
      rightOp.setChildOperators(child);

      List<Operator<? extends OperatorDesc>> parent = leftOp
          .getParentOperators();
      parent.add(rightOp);

      UnionDesc uDesc = ((UnionOperator) leftOp).getConf();
      uDesc.setNumInputs(uDesc.getNumInputs() + 1);
      return putOpInsertMap(leftOp, unionoutRR);
    }

    if (rightOp instanceof UnionOperator ||
        (rightOp instanceof SelectOperator &&
         rightOp.getParentOperators() != null &&
         !rightOp.getParentOperators().isEmpty() &&
         rightOp.getParentOperators().get(0) instanceof UnionOperator &&
         ((SelectOperator)rightOp).isIdentitySelect()) ) {

      if(!(rightOp instanceof UnionOperator)) {
        Operator oldChild = rightOp;
        rightOp = (Operator) rightOp.getParentOperators().get(0);
        rightOp.removeChildAndAdoptItsChildren(oldChild);
      }

      // make right a child of left
      List<Operator<? extends OperatorDesc>> child =
          new ArrayList<Operator<? extends OperatorDesc>>();
      child.add(rightOp);
      leftOp.setChildOperators(child);

      List<Operator<? extends OperatorDesc>> parent = rightOp
          .getParentOperators();
      parent.add(leftOp);
      UnionDesc uDesc = ((UnionOperator) rightOp).getConf();
      uDesc.setNumInputs(uDesc.getNumInputs() + 1);

      return putOpInsertMap(rightOp, unionoutRR);
    }

    // Create a new union operator
    Operator<? extends OperatorDesc> unionforward = OperatorFactory
        .getAndMakeChild(new UnionDesc(), new RowSchema(unionoutRR
            .getColumnInfos()));

    // set union operator as child of each of leftOp and rightOp
    List<Operator<? extends OperatorDesc>> child =
        new ArrayList<Operator<? extends OperatorDesc>>();
    child.add(unionforward);
    rightOp.setChildOperators(child);

    child = new ArrayList<Operator<? extends OperatorDesc>>();
    child.add(unionforward);
    leftOp.setChildOperators(child);

    List<Operator<? extends OperatorDesc>> parent =
        new ArrayList<Operator<? extends OperatorDesc>>();
    parent.add(leftOp);
    parent.add(rightOp);
    unionforward.setParentOperators(parent);

    // create operator info list to return
    return putOpInsertMap(unionforward, unionoutRR);
  }

  /**
   * Generates a select operator which can go between the original input operator and the union
   * operator. This select casts columns to match the type of the associated column in the union,
   * other columns pass through unchanged. The new operator's only parent is the original input
   * operator to the union, and it's only child is the union. If the input does not need to be
   * cast, the original operator is returned, and no new select operator is added.
   *
   * @param origInputOp
   *          The original input operator to the union.
   * @param origInputFieldMap
   *          A map from field name to ColumnInfo for the original input operator.
   * @param origInputAlias
   *          The alias associated with the original input operator.
   * @param unionoutRR
   *          The union's output row resolver.
   * @param unionalias
   *          The alias of the union.
   * @return
   * @throws SemanticException
   */
  private Operator<? extends OperatorDesc> genInputSelectForUnion(
      Operator<? extends OperatorDesc> origInputOp, Map<String, ColumnInfo> origInputFieldMap,
      String origInputAlias, RowResolver unionoutRR, String unionalias)
      throws SemanticException {

    List<ExprNodeDesc> columns = new ArrayList<ExprNodeDesc>();
    boolean needsCast = false;
    for (Map.Entry<String, ColumnInfo> unionEntry : unionoutRR.getFieldMap(unionalias).entrySet()) {
      String field = unionEntry.getKey();
      ColumnInfo lInfo = origInputFieldMap.get(field);
      ExprNodeDesc column = new ExprNodeColumnDesc(lInfo.getType(), lInfo.getInternalName(),
          lInfo.getTabAlias(), lInfo.getIsVirtualCol(), lInfo.isSkewedCol());
      if (!lInfo.getType().equals(unionEntry.getValue().getType())) {
        needsCast = true;
        column = ParseUtils.createConversionCast(
            column, (PrimitiveTypeInfo)unionEntry.getValue().getType());
      }
      columns.add(column);
    }

    // If none of the columns need to be cast there's no need for an additional select operator
    if (!needsCast) {
      return origInputOp;
    }

    RowResolver rowResolver = new RowResolver();
    Map<String, ExprNodeDesc> columnExprMap = new HashMap<String, ExprNodeDesc>();

    List<String> colName = new ArrayList<String>();
    for (int i = 0; i < columns.size(); i++) {
      String name = getColumnInternalName(i);
      ColumnInfo col = new ColumnInfo(name, columns.get(i)
          .getTypeInfo(), "", false);
      rowResolver.put(origInputAlias, name, col);
      colName.add(name);
      columnExprMap.put(name, columns.get(i));
    }

    Operator<SelectDesc> newInputOp = OperatorFactory.getAndMakeChild(
        new SelectDesc(columns, colName), new RowSchema(rowResolver.getColumnInfos()),
        columnExprMap, origInputOp);
    return putOpInsertMap(newInputOp, rowResolver);
  }

  /**
   * Generates the sampling predicate from the TABLESAMPLE clause information.
   * This function uses the bucket column list to decide the expression inputs
   * to the predicate hash function in case useBucketCols is set to true,
   * otherwise the expression list stored in the TableSample is used. The bucket
   * columns of the table are used to generate this predicate in case no
   * expressions are provided on the TABLESAMPLE clause and the table has
   * clustering columns defined in it's metadata. The predicate created has the
   * following structure:
   *
   * ((hash(expressions) & Integer.MAX_VALUE) % denominator) == numerator
   *
   * @param ts
   *          TABLESAMPLE clause information
   * @param bucketCols
   *          The clustering columns of the table
   * @param useBucketCols
   *          Flag to indicate whether the bucketCols should be used as input to
   *          the hash function
   * @param alias
   *          The alias used for the table in the row resolver
   * @param rwsch
   *          The row resolver used to resolve column references
   * @param qbm
   *          The metadata information for the query block which is used to
   *          resolve unaliased columns
   * @param planExpr
   *          The plan tree for the expression. If the user specified this, the
   *          parse expressions are not used
   * @return exprNodeDesc
   * @exception SemanticException
   */
  private ExprNodeDesc genSamplePredicate(TableSample ts,
      List<String> bucketCols, boolean useBucketCols, String alias,
      RowResolver rwsch, QBMetaData qbm, ExprNodeDesc planExpr)
      throws SemanticException {

    ExprNodeDesc numeratorExpr = new ExprNodeConstantDesc(
        TypeInfoFactory.intTypeInfo, Integer.valueOf(ts.getNumerator() - 1));

    ExprNodeDesc denominatorExpr = new ExprNodeConstantDesc(
        TypeInfoFactory.intTypeInfo, Integer.valueOf(ts.getDenominator()));

    ExprNodeDesc intMaxExpr = new ExprNodeConstantDesc(
        TypeInfoFactory.intTypeInfo, Integer.valueOf(Integer.MAX_VALUE));

    ArrayList<ExprNodeDesc> args = new ArrayList<ExprNodeDesc>();
    if (planExpr != null) {
      args.add(planExpr);
    } else if (useBucketCols) {
      for (String col : bucketCols) {
        ColumnInfo ci = rwsch.get(alias, col);
        // TODO: change type to the one in the table schema
        args.add(new ExprNodeColumnDesc(ci));
      }
    } else {
      for (ASTNode expr : ts.getExprs()) {
        args.add(genExprNodeDesc(expr, rwsch));
      }
    }

    ExprNodeDesc equalsExpr = null;
    {
      ExprNodeDesc hashfnExpr = new ExprNodeGenericFuncDesc(
          TypeInfoFactory.intTypeInfo, new GenericUDFHash(), args);
      assert (hashfnExpr != null);
      LOG.info("hashfnExpr = " + hashfnExpr);
      ExprNodeDesc andExpr = TypeCheckProcFactory.DefaultExprProcessor
          .getFuncExprNodeDesc("&", hashfnExpr, intMaxExpr);
      assert (andExpr != null);
      LOG.info("andExpr = " + andExpr);
      ExprNodeDesc modExpr = TypeCheckProcFactory.DefaultExprProcessor
          .getFuncExprNodeDesc("%", andExpr, denominatorExpr);
      assert (modExpr != null);
      LOG.info("modExpr = " + modExpr);
      LOG.info("numeratorExpr = " + numeratorExpr);
      equalsExpr = TypeCheckProcFactory.DefaultExprProcessor
          .getFuncExprNodeDesc("==", modExpr, numeratorExpr);
      LOG.info("equalsExpr = " + equalsExpr);
      assert (equalsExpr != null);
    }
    return equalsExpr;
  }

  private String getAliasId(String alias, QB qb) {
    return (qb.getId() == null ? alias : qb.getId() + ":" + alias).toLowerCase();
  }

  @SuppressWarnings("nls")
  private Operator genTablePlan(String alias, QB qb) throws SemanticException {

    String alias_id = getAliasId(alias, qb);
    Table tab = qb.getMetaData().getSrcForAlias(alias);
    RowResolver rwsch;

    // is the table already present
    Operator<? extends OperatorDesc> top = topOps.get(alias_id);
    Operator<? extends OperatorDesc> dummySel = topSelOps.get(alias_id);
    if (dummySel != null) {
      top = dummySel;
    }

    if (top == null) {
      rwsch = new RowResolver();
      try {
        StructObjectInspector rowObjectInspector = (StructObjectInspector) tab
            .getDeserializer().getObjectInspector();
        List<? extends StructField> fields = rowObjectInspector
            .getAllStructFieldRefs();
        for (int i = 0; i < fields.size(); i++) {
          /**
           * if the column is a skewed column, use ColumnInfo accordingly
           */
          ColumnInfo colInfo = new ColumnInfo(fields.get(i).getFieldName(),
              TypeInfoUtils.getTypeInfoFromObjectInspector(fields.get(i)
                  .getFieldObjectInspector()), alias, false);
          colInfo.setSkewedCol((isSkewedCol(alias, qb, fields.get(i)
              .getFieldName())) ? true : false);
          rwsch.put(alias, fields.get(i).getFieldName(), colInfo);
        }
      } catch (SerDeException e) {
        throw new RuntimeException(e);
      }
      // Hack!! - refactor once the metadata APIs with types are ready
      // Finally add the partitioning columns
      for (FieldSchema part_col : tab.getPartCols()) {
        LOG.trace("Adding partition col: " + part_col);
        rwsch.put(alias, part_col.getName(), new ColumnInfo(part_col.getName(),
            TypeInfoFactory.getPrimitiveTypeInfo(part_col.getType()), alias, true));
      }

      // put all virtual columns in RowResolver.
      Iterator<VirtualColumn> vcs = VirtualColumn.getRegistry(conf).iterator();
      // use a list for easy cumtomize
      List<VirtualColumn> vcList = new ArrayList<VirtualColumn>();
      while (vcs.hasNext()) {
        VirtualColumn vc = vcs.next();
        rwsch.put(alias, vc.getName(), new ColumnInfo(vc.getName(),
            vc.getTypeInfo(), alias, true, vc.getIsHidden()));
        vcList.add(vc);
      }

      // Create the root of the operator tree
      TableScanDesc tsDesc = new TableScanDesc(alias, vcList, tab);
      setupStats(tsDesc, qb.getParseInfo(), tab, alias, rwsch);

      SplitSample sample = nameToSplitSample.get(alias_id);
      if (sample != null && sample.getRowCount() != null) {
        tsDesc.setRowLimit(sample.getRowCount());
        nameToSplitSample.remove(alias_id);
      }

      top = putOpInsertMap(OperatorFactory.get(tsDesc,
          new RowSchema(rwsch.getColumnInfos())), rwsch);

      // Add this to the list of top operators - we always start from a table
      // scan
      topOps.put(alias_id, top);

      // Add a mapping from the table scan operator to Table
      topToTable.put((TableScanOperator) top, tab);

      Map<String, String> props = qb.getTabPropsForAlias(alias);
      if (props != null) {
        topToTableProps.put((TableScanOperator) top, props);
        tsDesc.setOpProps(props);
      }
    } else {
      rwsch = opParseCtx.get(top).getRowResolver();
      top.setChildOperators(null);
    }

    // check if this table is sampled and needs more than input pruning
    Operator<? extends OperatorDesc> tableOp = top;
    TableSample ts = qb.getParseInfo().getTabSample(alias);
    if (ts != null) {
      int num = ts.getNumerator();
      int den = ts.getDenominator();
      ArrayList<ASTNode> sampleExprs = ts.getExprs();

      // TODO: Do the type checking of the expressions
      List<String> tabBucketCols = tab.getBucketCols();
      int numBuckets = tab.getNumBuckets();

      // If there are no sample cols and no bucket cols then throw an error
      if (tabBucketCols.size() == 0 && sampleExprs.size() == 0) {
        throw new SemanticException(ErrorMsg.NON_BUCKETED_TABLE.getMsg() + " "
            + tab.getTableName());
      }

      if (num > den) {
        throw new SemanticException(
            ErrorMsg.BUCKETED_NUMERATOR_BIGGER_DENOMINATOR.getMsg() + " "
                + tab.getTableName());
      }

      // check if a predicate is needed
      // predicate is needed if either input pruning is not enough
      // or if input pruning is not possible

      // check if the sample columns are the same as the table bucket columns
      boolean colsEqual = true;
      if ((sampleExprs.size() != tabBucketCols.size())
          && (sampleExprs.size() != 0)) {
        colsEqual = false;
      }

      for (int i = 0; i < sampleExprs.size() && colsEqual; i++) {
        boolean colFound = false;
        for (int j = 0; j < tabBucketCols.size() && !colFound; j++) {
          if (sampleExprs.get(i).getToken().getType() != HiveParser.TOK_TABLE_OR_COL) {
            break;
          }

          if (((ASTNode) sampleExprs.get(i).getChild(0)).getText()
              .equalsIgnoreCase(tabBucketCols.get(j))) {
            colFound = true;
          }
        }
        colsEqual = (colsEqual && colFound);
      }

      // Check if input can be pruned
      ts.setInputPruning((sampleExprs == null || sampleExprs.size() == 0 || colsEqual));

      // check if input pruning is enough
      if ((sampleExprs == null || sampleExprs.size() == 0 || colsEqual)
          && (num == den || (den % numBuckets == 0 || numBuckets % den == 0))) {

        // input pruning is enough; add the filter for the optimizer to use it
        // later
        LOG.info("No need for sample filter");
        ExprNodeDesc samplePredicate = genSamplePredicate(ts, tabBucketCols,
            colsEqual, alias, rwsch, qb.getMetaData(), null);
        tableOp = OperatorFactory.getAndMakeChild(new FilterDesc(
            samplePredicate, true, new sampleDesc(ts.getNumerator(), ts
                .getDenominator(), tabBucketCols, true)),
            new RowSchema(rwsch.getColumnInfos()), top);
      } else {
        // need to add filter
        // create tableOp to be filterDesc and set as child to 'top'
        LOG.info("Need sample filter");
        ExprNodeDesc samplePredicate = genSamplePredicate(ts, tabBucketCols,
            colsEqual, alias, rwsch, qb.getMetaData(), null);
        tableOp = OperatorFactory.getAndMakeChild(new FilterDesc(
            samplePredicate, true),
            new RowSchema(rwsch.getColumnInfos()), top);
      }
    } else {
      boolean testMode = conf.getBoolVar(HiveConf.ConfVars.HIVETESTMODE);
      if (testMode) {
        String tabName = tab.getTableName();

        // has the user explicitly asked not to sample this table
        String unSampleTblList = conf
            .getVar(HiveConf.ConfVars.HIVETESTMODENOSAMPLE);
        String[] unSampleTbls = unSampleTblList.split(",");
        boolean unsample = false;
        for (String unSampleTbl : unSampleTbls) {
          if (tabName.equalsIgnoreCase(unSampleTbl)) {
            unsample = true;
          }
        }

        if (!unsample) {
          int numBuckets = tab.getNumBuckets();

          // If the input table is bucketed, choose the first bucket
          if (numBuckets > 0) {
            TableSample tsSample = new TableSample(1, numBuckets);
            tsSample.setInputPruning(true);
            qb.getParseInfo().setTabSample(alias, tsSample);
            ExprNodeDesc samplePred = genSamplePredicate(tsSample, tab
                .getBucketCols(), true, alias, rwsch, qb.getMetaData(), null);
            tableOp = OperatorFactory
                .getAndMakeChild(new FilterDesc(samplePred, true,
                    new sampleDesc(tsSample.getNumerator(), tsSample
                        .getDenominator(), tab.getBucketCols(), true)),
                    new RowSchema(rwsch.getColumnInfos()), top);
            LOG.info("No need for sample filter");
          } else {
            // The table is not bucketed, add a dummy filter :: rand()
            int freq = conf.getIntVar(HiveConf.ConfVars.HIVETESTMODESAMPLEFREQ);
            TableSample tsSample = new TableSample(1, freq);
            tsSample.setInputPruning(false);
            qb.getParseInfo().setTabSample(alias, tsSample);
            LOG.info("Need sample filter");
            ExprNodeDesc randFunc = TypeCheckProcFactory.DefaultExprProcessor
                .getFuncExprNodeDesc("rand", new ExprNodeConstantDesc(Integer
                    .valueOf(460476415)));
            ExprNodeDesc samplePred = genSamplePredicate(tsSample, null, false,
                alias, rwsch, qb.getMetaData(), randFunc);
            tableOp = OperatorFactory.getAndMakeChild(new FilterDesc(
                samplePred, true),
                new RowSchema(rwsch.getColumnInfos()), top);
          }
        }
      }
    }

    Operator output = putOpInsertMap(tableOp, rwsch);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Created Table Plan for " + alias + " " + tableOp.toString());
    }

    return output;
  }

  static boolean isSkewedCol(String alias, QB qb, String colName) {
    boolean isSkewedCol = false;
    List<String> skewedCols = qb.getSkewedColumnNames(alias);
    for (String skewedCol : skewedCols) {
      if (skewedCol.equalsIgnoreCase(colName)) {
        isSkewedCol = true;
      }
    }
    return isSkewedCol;
  }

  private void setupStats(TableScanDesc tsDesc, QBParseInfo qbp, Table tab, String alias,
      RowResolver rwsch)
      throws SemanticException {

    if (!qbp.isAnalyzeCommand()) {
      tsDesc.setGatherStats(false);
    } else {
      if (HiveConf.getVar(conf, HIVESTATSDBCLASS).equalsIgnoreCase(StatDB.fs.name())) {
        String statsTmpLoc = ctx.getExtTmpPathRelTo(tab.getPath()).toString();
        LOG.info("Set stats collection dir : " + statsTmpLoc);
        conf.set(StatsSetupConst.STATS_TMP_LOC, statsTmpLoc);
      }
      tsDesc.setGatherStats(true);
      tsDesc.setStatsReliable(conf.getBoolVar(HiveConf.ConfVars.HIVE_STATS_RELIABLE));
      tsDesc.setMaxStatsKeyPrefixLength(StatsFactory.getMaxPrefixLength(conf));

      // append additional virtual columns for storing statistics
      Iterator<VirtualColumn> vcs = VirtualColumn.getStatsRegistry(conf).iterator();
      List<VirtualColumn> vcList = new ArrayList<VirtualColumn>();
      while (vcs.hasNext()) {
        VirtualColumn vc = vcs.next();
        rwsch.put(alias, vc.getName(), new ColumnInfo(vc.getName(),
            vc.getTypeInfo(), alias, true, vc.getIsHidden()));
        vcList.add(vc);
      }
      tsDesc.addVirtualCols(vcList);

      String tblName = tab.getTableName();
      tableSpec tblSpec = qbp.getTableSpec(alias);
      Map<String, String> partSpec = tblSpec.getPartSpec();

      if (partSpec != null) {
        List<String> cols = new ArrayList<String>();
        cols.addAll(partSpec.keySet());
        tsDesc.setPartColumns(cols);
      }

      // Theoretically the key prefix could be any unique string shared
      // between TableScanOperator (when publishing) and StatsTask (when aggregating).
      // Here we use
      // db_name.table_name + partitionSec
      // as the prefix for easy of read during explain and debugging.
      // Currently, partition spec can only be static partition.
      String k = tblName + Path.SEPARATOR;
      tsDesc.setStatsAggPrefix(tab.getDbName()+"."+k);

      // set up WriteEntity for replication
      outputs.add(new WriteEntity(tab, WriteEntity.WriteType.DDL_SHARED));

      // add WriteEntity for each matching partition
      if (tab.isPartitioned()) {
        if (partSpec == null) {
          throw new SemanticException(ErrorMsg.NEED_PARTITION_SPECIFICATION.getMsg());
        }
        List<Partition> partitions = qbp.getTableSpec().partitions;
        if (partitions != null) {
          for (Partition partn : partitions) {
            // inputs.add(new ReadEntity(partn)); // is this needed at all?
            outputs.add(new WriteEntity(partn, WriteEntity.WriteType.DDL_NO_LOCK));
          }
        }
      }
    }
  }

  private Operator genPlan(QB parent, QBExpr qbexpr) throws SemanticException {
    if (qbexpr.getOpcode() == QBExpr.Opcode.NULLOP) {
      boolean skipAmbiguityCheck = viewSelect == null && parent.isTopLevelSelectStarQuery();
      return genPlan(qbexpr.getQB(), skipAmbiguityCheck);
    }
    if (qbexpr.getOpcode() == QBExpr.Opcode.UNION) {
      Operator qbexpr1Ops = genPlan(parent, qbexpr.getQBExpr1());
      Operator qbexpr2Ops = genPlan(parent, qbexpr.getQBExpr2());

      return genUnionPlan(qbexpr.getAlias(), qbexpr.getQBExpr1().getAlias(),
          qbexpr1Ops, qbexpr.getQBExpr2().getAlias(), qbexpr2Ops);
    }
    return null;
  }

  public Operator genPlan(QB qb) throws SemanticException {
    return genPlan(qb, false);
  }

  @SuppressWarnings("nls")
  public Operator genPlan(QB qb, boolean skipAmbiguityCheck)
      throws SemanticException {

    // First generate all the opInfos for the elements in the from clause
    // Must be deterministic order map - see HIVE-8707
    Map<String, Operator> aliasToOpInfo = new LinkedHashMap<String, Operator>();

    // Recurse over the subqueries to fill the subquery part of the plan
    for (String alias : qb.getSubqAliases()) {
      QBExpr qbexpr = qb.getSubqForAlias(alias);
      aliasToOpInfo.put(alias, genPlan(qb, qbexpr));
    }

    // Recurse over all the source tables
    for (String alias : qb.getTabAliases()) {
      Operator op = genTablePlan(alias, qb);
      aliasToOpInfo.put(alias, op);
    }

    if (aliasToOpInfo.isEmpty()) {
      qb.getMetaData().setSrcForAlias(DUMMY_TABLE, getDummyTable());
      TableScanOperator op = (TableScanOperator) genTablePlan(DUMMY_TABLE, qb);
      op.getConf().setRowLimit(1);
      qb.addAlias(DUMMY_TABLE);
      qb.setTabAlias(DUMMY_TABLE, DUMMY_TABLE);
      aliasToOpInfo.put(DUMMY_TABLE, op);
    }

    Operator srcOpInfo = null;
    Operator lastPTFOp = null;

    if(queryProperties.hasPTF()){
      //After processing subqueries and source tables, process
      // partitioned table functions

      HashMap<ASTNode, PTFInvocationSpec> ptfNodeToSpec = qb.getPTFNodeToSpec();
      if ( ptfNodeToSpec != null ) {
        for(Entry<ASTNode, PTFInvocationSpec> entry : ptfNodeToSpec.entrySet()) {
          ASTNode ast = entry.getKey();
          PTFInvocationSpec spec = entry.getValue();
          String inputAlias = spec.getQueryInputName();
          Operator inOp = aliasToOpInfo.get(inputAlias);
          if ( inOp == null ) {
            throw new SemanticException(generateErrorMessage(ast,
                "Cannot resolve input Operator for PTF invocation"));
          }
          lastPTFOp = genPTFPlan(spec, inOp);
          String ptfAlias = spec.getFunction().getAlias();
          if ( ptfAlias != null ) {
            aliasToOpInfo.put(ptfAlias, lastPTFOp);
          }
        }
      }

    }

    // For all the source tables that have a lateral view, attach the
    // appropriate operators to the TS
    genLateralViewPlans(aliasToOpInfo, qb);


    // process join
    if (qb.getParseInfo().getJoinExpr() != null) {
      ASTNode joinExpr = qb.getParseInfo().getJoinExpr();

      if (joinExpr.getToken().getType() == HiveParser.TOK_UNIQUEJOIN) {
        QBJoinTree joinTree = genUniqueJoinTree(qb, joinExpr, aliasToOpInfo);
        qb.setQbJoinTree(joinTree);
      } else {
        QBJoinTree joinTree = genJoinTree(qb, joinExpr, aliasToOpInfo);
        qb.setQbJoinTree(joinTree);
        /*
         * if there is only one destination in Query try to push where predicates
         * as Join conditions
         */
        Set<String> dests = qb.getParseInfo().getClauseNames();
        if ( dests.size() == 1 ) {
          String dest = dests.iterator().next();
          ASTNode whereClause = qb.getParseInfo().getWhrForClause(dest);
          if ( whereClause != null ) {
            extractJoinCondsFromWhereClause(joinTree, qb, dest,
                (ASTNode) whereClause.getChild(0),
                aliasToOpInfo );
          }
        }

        if (!disableJoinMerge)
          mergeJoinTree(qb);
      }

      // if any filters are present in the join tree, push them on top of the
      // table
      pushJoinFilters(qb, qb.getQbJoinTree(), aliasToOpInfo);
      srcOpInfo = genJoinPlan(qb, aliasToOpInfo);
    } else {
      // Now if there are more than 1 sources then we have a join case
      // later we can extend this to the union all case as well
      srcOpInfo = aliasToOpInfo.values().iterator().next();
      // with ptfs, there maybe more (note for PTFChains:
      // 1 ptf invocation may entail multiple PTF operators)
      srcOpInfo = lastPTFOp != null ? lastPTFOp : srcOpInfo;
    }

    Operator bodyOpInfo = genBodyPlan(qb, srcOpInfo, aliasToOpInfo);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Created Plan for Query Block " + qb.getId());
    }

    if (qb.getAlias() != null) {
      rewriteRRForSubQ(qb.getAlias(), bodyOpInfo, skipAmbiguityCheck);
    }

    setQB(qb);
    return bodyOpInfo;
  }

  // change curr ops row resolver's tab aliases to subq alias
  private void rewriteRRForSubQ(String alias, Operator operator, boolean skipAmbiguityCheck)
      throws SemanticException {
    RowResolver rr = opParseCtx.get(operator).getRowResolver();
    RowResolver newRR = new RowResolver();
    for (ColumnInfo colInfo : rr.getColumnInfos()) {
      String name = colInfo.getInternalName();
      String[] tmp = rr.reverseLookup(name);
      if ("".equals(tmp[0]) || tmp[1] == null) {
        // ast expression is not a valid column name for table
        tmp[1] = colInfo.getInternalName();
      } else if (newRR.get(alias, tmp[1]) != null) {
        // enforce uniqueness of column names
        if (!skipAmbiguityCheck) {
          throw new SemanticException(ErrorMsg.AMBIGUOUS_COLUMN.getMsg(tmp[1] + " in " + alias));
        }
        // if it's wrapped by top-level select star query, skip ambiguity check (for backward compatibility)
        tmp[1] = colInfo.getInternalName();
      }
      newRR.put(alias, tmp[1], colInfo);
    }
    opParseCtx.get(operator).setRowResolver(newRR);
  }

  private Table getDummyTable() throws SemanticException {
    Path dummyPath = createDummyFile();
    Table desc = new Table(DUMMY_DATABASE, DUMMY_TABLE);
    desc.getTTable().getSd().setLocation(dummyPath.toString());
    desc.getTTable().getSd().getSerdeInfo().setSerializationLib(NullStructSerDe.class.getName());
    desc.setInputFormatClass(NullRowsInputFormat.class);
    desc.setOutputFormatClass(HiveIgnoreKeyTextOutputFormat.class);
    return desc;
  }

  // add dummy data for not removed by CombineHiveInputFormat, etc.
  private Path createDummyFile() throws SemanticException {
    Path dummyPath = new Path(ctx.getMRScratchDir(), "dummy_path");
    Path dummyFile = new Path(dummyPath, "dummy_file");
    FSDataOutputStream fout = null;
    try {
      FileSystem fs = dummyFile.getFileSystem(conf);
      if (fs.exists(dummyFile)) {
        return dummyPath;
      }
      fout = fs.create(dummyFile);
      fout.write(1);
      fout.close();
    } catch (IOException e) {
      throw new SemanticException(e);
    } finally {
      IOUtils.closeStream(fout);
    }
    return dummyPath;
  }

  /**
   * Generates the operator DAG needed to implement lateral views and attaches
   * it to the TS operator.
   *
   * @param aliasToOpInfo
   *          A mapping from a table alias to the TS operator. This function
   *          replaces the operator mapping as necessary
   * @param qb
   * @throws SemanticException
   */

  void genLateralViewPlans(Map<String, Operator> aliasToOpInfo, QB qb)
      throws SemanticException {
    Map<String, ArrayList<ASTNode>> aliasToLateralViews = qb.getParseInfo()
        .getAliasToLateralViews();
    for (Entry<String, Operator> e : aliasToOpInfo.entrySet()) {
      String alias = e.getKey();
      // See if the alias has a lateral view. If so, chain the lateral view
      // operator on
      ArrayList<ASTNode> lateralViews = aliasToLateralViews.get(alias);
      if (lateralViews != null) {
        Operator op = e.getValue();

        for (ASTNode lateralViewTree : aliasToLateralViews.get(alias)) {
          // There are 2 paths from the TS operator (or a previous LVJ operator)
          // to the same LateralViewJoinOperator.
          // TS -> SelectOperator(*) -> LateralViewJoinOperator
          // TS -> SelectOperator (gets cols for UDTF) -> UDTFOperator0
          // -> LateralViewJoinOperator
          //

          Operator lateralViewJoin = genLateralViewPlan(qb, op, lateralViewTree);
          op = lateralViewJoin;
        }
        e.setValue(op);
      }
    }
  }

  private Operator genLateralViewPlanForDest(String dest, QB qb, Operator op)
      throws SemanticException {
    ASTNode lateralViewTree = qb.getParseInfo().getDestToLateralView().get(dest);
    if (lateralViewTree != null) {
      return genLateralViewPlan(qb, op, lateralViewTree);
    }
    return op;
  }

  private Operator genLateralViewPlan(QB qb, Operator op, ASTNode lateralViewTree)
      throws SemanticException {
    RowResolver lvForwardRR = new RowResolver();
    RowResolver source = opParseCtx.get(op).getRowResolver();
    Map<String, ExprNodeDesc> lvfColExprMap = new HashMap<String, ExprNodeDesc>();
    Map<String, ExprNodeDesc> selColExprMap = new HashMap<String, ExprNodeDesc>();
    List<ExprNodeDesc> colList = new ArrayList<ExprNodeDesc>();
    List<String> colNames = new ArrayList<String>();
    for (ColumnInfo col : source.getColumnInfos()) {
      String[] tabCol = source.reverseLookup(col.getInternalName());
      lvForwardRR.put(tabCol[0], tabCol[1], col);
      ExprNodeDesc colExpr = new ExprNodeColumnDesc(col);
      colList.add(colExpr);
      colNames.add(colExpr.getName());
      lvfColExprMap.put(col.getInternalName(), colExpr);
      selColExprMap.put(col.getInternalName(), colExpr.clone());
    }

    Operator lvForward = putOpInsertMap(OperatorFactory.getAndMakeChild(
        new LateralViewForwardDesc(), new RowSchema(lvForwardRR.getColumnInfos()),
        op), lvForwardRR);
    lvForward.setColumnExprMap(lvfColExprMap);

    // The order in which the two paths are added is important. The
    // lateral view join operator depends on having the select operator
    // give it the row first.

    // Get the all path by making a select(*).
    RowResolver allPathRR = opParseCtx.get(lvForward).getRowResolver();
    // Operator allPath = op;
    SelectDesc sDesc = new SelectDesc(colList, colNames, false);
    sDesc.setSelStarNoCompute(true);
    Operator allPath = putOpInsertMap(OperatorFactory.getAndMakeChild(
        sDesc, new RowSchema(allPathRR.getColumnInfos()),
        lvForward), allPathRR);
    allPath.setColumnExprMap(selColExprMap);
    int allColumns = allPathRR.getColumnInfos().size();
    // Get the UDTF Path
    QB blankQb = new QB(null, null, false);
    Operator udtfPath = genSelectPlan((ASTNode) lateralViewTree
        .getChild(0), blankQb, lvForward, null,
        lateralViewTree.getType() == HiveParser.TOK_LATERAL_VIEW_OUTER);
    // add udtf aliases to QB
    for (String udtfAlias : blankQb.getAliases()) {
      qb.addAlias(udtfAlias);
    }
    RowResolver udtfPathRR = opParseCtx.get(udtfPath).getRowResolver();

    // Merge the two into the lateral view join
    // The cols of the merged result will be the combination of both the
    // cols of the UDTF path and the cols of the all path. The internal
    // names have to be changed to avoid conflicts

    RowResolver lateralViewRR = new RowResolver();
    ArrayList<String> outputInternalColNames = new ArrayList<String>();


    // For PPD, we need a column to expression map so that during the walk,
    // the processor knows how to transform the internal col names.
    // Following steps are dependant on the fact that we called
    // LVmerge.. in the above order
    Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();

    LVmergeRowResolvers(allPathRR, lateralViewRR, colExprMap, outputInternalColNames);
    LVmergeRowResolvers(udtfPathRR, lateralViewRR, colExprMap, outputInternalColNames);

    Operator lateralViewJoin = putOpInsertMap(OperatorFactory
        .getAndMakeChild(new LateralViewJoinDesc(allColumns, outputInternalColNames),
            new RowSchema(lateralViewRR.getColumnInfos()), allPath,
            udtfPath), lateralViewRR);
    lateralViewJoin.setColumnExprMap(colExprMap);
    return lateralViewJoin;
  }

  /**
   * A helper function that gets all the columns and respective aliases in the
   * source and puts them into dest. It renames the internal names of the
   * columns based on getColumnInternalName(position).
   *
   * Note that this helper method relies on RowResolver.getColumnInfos()
   * returning the columns in the same order as they will be passed in the
   * operator DAG.
   *
   * @param source
   * @param dest
   * @param outputColNames
   *          - a list to which the new internal column names will be added, in
   *          the same order as in the dest row resolver
   */
  private void LVmergeRowResolvers(RowResolver source, RowResolver dest,
      Map<String, ExprNodeDesc> colExprMap, ArrayList<String> outputInternalColNames) {
    for (ColumnInfo c : source.getColumnInfos()) {
      String internalName = getColumnInternalName(outputInternalColNames.size());
      outputInternalColNames.add(internalName);
      ColumnInfo newCol = new ColumnInfo(internalName, c.getType(), c
          .getTabAlias(), c.getIsVirtualCol(), c.isHiddenVirtualCol());
      String[] tableCol = source.reverseLookup(c.getInternalName());
      String tableAlias = tableCol[0];
      String colAlias = tableCol[1];
      dest.put(tableAlias, colAlias, newCol);
      colExprMap.put(internalName, new ExprNodeColumnDesc(c));
    }
  }

  @SuppressWarnings("nls")
  public Phase1Ctx initPhase1Ctx() {

    Phase1Ctx ctx_1 = new Phase1Ctx();
    ctx_1.nextNum = 0;
    ctx_1.dest = "reduce";

    return ctx_1;
  }

  @Override
  public void init(boolean clearPartsCache) {
    // clear most members
    reset(clearPartsCache);

    // init
    QB qb = new QB(null, null, false);
    this.qb = qb;
  }

  boolean analyzeCreateTable(ASTNode child) throws SemanticException {
    if (ast.getToken().getType() == HiveParser.TOK_CREATETABLE) {
      // if it is not CTAS, we don't need to go further and just return
      if ((child = analyzeCreateTable(ast, qb, null)) == null) {
        return true;
      }
    } else {
      SessionState.get().setCommandType(HiveOperation.QUERY);
    }

    return false;
  }

  @Override
  @SuppressWarnings("nls")
  public void analyzeInternal(ASTNode ast) throws SemanticException {
    analyzeInternal(ast, new PlannerContext());
  }

  /**
   * Planner specific stuff goen in here.
   */
  static class PlannerContext {
    protected ASTNode   child;
    protected Phase1Ctx ctx_1;

    void setParseTreeAttr(ASTNode child, Phase1Ctx ctx_1) {
      this.child = child;
      this.ctx_1 = ctx_1;
    }

    void setCTASToken(ASTNode child) {
    }

    void setInsertToken(ASTNode ast, boolean isTmpFileDest) {
    }
  }

  boolean genResolvedParseTree(ASTNode ast, PlannerContext plannerCtx) throws SemanticException {
    ASTNode child = ast;
    this.ast = ast;
    viewsExpanded = new ArrayList<String>();
    ctesExpanded = new ArrayList<String>();

    // 1. analyze and process the position alias
    processPositionAlias(ast);

    // 2. analyze create table command
    if (ast.getToken().getType() == HiveParser.TOK_CREATETABLE) {
      // if it is not CTAS, we don't need to go further and just return
      if ((child = analyzeCreateTable(ast, qb, plannerCtx)) == null) {
        return false;
      }
    } else {
      SessionState.get().setCommandType(HiveOperation.QUERY);
    }

    // 3. analyze create view command
    if (ast.getToken().getType() == HiveParser.TOK_CREATEVIEW
        || (ast.getToken().getType() == HiveParser.TOK_ALTERVIEW && ast.getChild(1).getType() == HiveParser.TOK_QUERY)) {
      child = analyzeCreateView(ast, qb);
      SessionState.get().setCommandType(HiveOperation.CREATEVIEW);
      if (child == null) {
        return false;
      }
      viewSelect = child;
      // prevent view from referencing itself
      viewsExpanded.add(createVwDesc.getViewName());
    }

    // 4. continue analyzing from the child ASTNode.
    Phase1Ctx ctx_1 = initPhase1Ctx();
    if (!doPhase1(child, qb, ctx_1, plannerCtx)) {
      // if phase1Result false return
      return false;
    }
    LOG.info("Completed phase 1 of Semantic Analysis");

    // 5. Resolve Parse Tree
    getMetaData(qb);
    LOG.info("Completed getting MetaData in Semantic Analysis");

    plannerCtx.setParseTreeAttr(child, ctx_1);

    return true;
  }

  Operator genOPTree(ASTNode ast, PlannerContext plannerCtx) throws SemanticException {
    return genPlan(qb);
  }

  void analyzeInternal(ASTNode ast, PlannerContext plannerCtx) throws SemanticException {
    // 1. Generate Resolved Parse tree from syntax tree
    LOG.info("Starting Semantic Analysis");
    if (!genResolvedParseTree(ast, plannerCtx)) {
      return;
    }

    // 2. Gen OP Tree from resolved Parse Tree
    Operator sinkOp = genOPTree(ast, plannerCtx);

    // 3. Deduce Resultset Schema
    if (createVwDesc != null) {
      resultSchema = convertRowSchemaToViewSchema(opParseCtx.get(sinkOp).getRowResolver());
    } else {
      resultSchema = convertRowSchemaToResultSetSchema(opParseCtx.get(sinkOp).getRowResolver(),
          HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_RESULTSET_USE_UNIQUE_COLUMN_NAMES));
    }

    // 4. Generate Parse Context for Optimizer & Physical compiler
    ParseContext pCtx = new ParseContext(conf, qb, plannerCtx.child,
        opToPartPruner, opToPartList, topOps, opParseCtx,
        new HashSet<JoinOperator>(joinContext.keySet()),
        new HashSet<SMBMapJoinOperator>(smbMapJoinContext.keySet()),
        loadTableWork, loadFileWork, ctx, idToTableNameMap, destTableId, uCtx,
        listMapJoinOpsNoReducer, groupOpToInputTables, prunedPartitions, opToSamplePruner,
        globalLimitCtx, nameToSplitSample, inputs, rootTasks, opToPartToSkewedPruner,
        viewAliasToInput, reduceSinkOperatorsAddedByEnforceBucketingSorting, queryProperties);

    // 5. Take care of view creation
    if (createVwDesc != null) {
      saveViewDefinition();

      // validate the create view statement at this point, the createVwDesc gets
      // all the information for semanticcheck
      validateCreateView(createVwDesc);

      // Since we're only creating a view (not executing it), we don't need to
      // optimize or translate the plan (and in fact, those procedures can
      // interfere with the view creation). So skip the rest of this method.
      ctx.setResDir(null);
      ctx.setResFile(null);

      try {
        PlanUtils.addInputsForView(pCtx);
      } catch (HiveException e) {
        throw new SemanticException(e);
      }
      return;
    }

    // 6. Generate table access stats if required
    if (HiveConf.getBoolVar(this.conf, HiveConf.ConfVars.HIVE_STATS_COLLECT_TABLEKEYS) == true) {
      TableAccessAnalyzer tableAccessAnalyzer = new TableAccessAnalyzer(pCtx);
      setTableAccessInfo(tableAccessAnalyzer.analyzeTableAccess());
    }

    // 7. Perform Logical optimization
    if (LOG.isDebugEnabled()) {
      LOG.debug("Before logical optimization\n" + Operator.toString(pCtx.getTopOps().values()));
    }
    Optimizer optm = new Optimizer();
    optm.setPctx(pCtx);
    optm.initialize(conf);
    pCtx = optm.optimize();
    FetchTask origFetchTask = pCtx.getFetchTask();
    if (LOG.isDebugEnabled()) {
      LOG.debug("After logical optimization\n" + Operator.toString(pCtx.getTopOps().values()));
    }

    // 8. Generate column access stats if required - wait until column pruning
    // takes place during optimization
    boolean isColumnInfoNeedForAuth = SessionState.get().isAuthorizationModeV2()
        && HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_AUTHORIZATION_ENABLED);
    if (isColumnInfoNeedForAuth
        || HiveConf.getBoolVar(this.conf, HiveConf.ConfVars.HIVE_STATS_COLLECT_SCANCOLS) == true) {
      ColumnAccessAnalyzer columnAccessAnalyzer = new ColumnAccessAnalyzer(pCtx);
      setColumnAccessInfo(columnAccessAnalyzer.analyzeColumnAccess());
    }

    // 9. Optimize Physical op tree & Translate to target execution engine (MR,
    // TEZ..)
    if (!ctx.getExplainLogical()) {
      TaskCompiler compiler = TaskCompilerFactory.getCompiler(conf, pCtx);
      compiler.init(conf, console, db);
      compiler.compile(pCtx, rootTasks, inputs, outputs);
      fetchTask = pCtx.getFetchTask();
    }
    LOG.info("Completed plan generation");

    // 10. put accessed columns to readEntity
    if (HiveConf.getBoolVar(this.conf, HiveConf.ConfVars.HIVE_STATS_COLLECT_SCANCOLS)) {
      putAccessedColumnsToReadEntity(inputs, columnAccessInfo);
    }

    // 11. if desired check we're not going over partition scan limits
    if (!ctx.getExplain()) {
      enforceScanLimits(pCtx, origFetchTask);
    }

    return;
  }

  private void putAccessedColumnsToReadEntity(HashSet<ReadEntity> inputs, ColumnAccessInfo columnAccessInfo) {
    Map<String, List<String>> tableToColumnAccessMap = columnAccessInfo.getTableToColumnAccessMap();
    if (tableToColumnAccessMap != null && !tableToColumnAccessMap.isEmpty()) {
      for(ReadEntity entity: inputs) {
        List<String> cols;
        switch (entity.getType()) {
          case TABLE:
            cols = tableToColumnAccessMap.get(entity.getTable().getCompleteName());
            if (cols != null && !cols.isEmpty()) {
              entity.getAccessedColumns().addAll(cols);
            }
            break;
          case PARTITION:
            cols = tableToColumnAccessMap.get(entity.getPartition().getTable().getCompleteName());
            if (cols != null && !cols.isEmpty()) {
              entity.getAccessedColumns().addAll(cols);
            }
            break;
          default:
            // no-op
        }
      }
    }
  }

  private void enforceScanLimits(ParseContext pCtx, FetchTask fTask)
      throws SemanticException {
    int scanLimit = HiveConf.getIntVar(conf, HiveConf.ConfVars.HIVELIMITTABLESCANPARTITION);

    if (scanLimit > -1) {
      // a scan limit on the number of partitions has been set by the user
      if (fTask != null) {
        // having a fetch task at this point means that we're not going to
        // launch a job on the cluster
        if (!fTask.getWork().isNotPartitioned() && fTask.getWork().getLimit() == -1
            && scanLimit < fTask.getWork().getPartDir().size()) {
          throw new SemanticException(ErrorMsg.PARTITION_SCAN_LIMIT_EXCEEDED, ""
              + fTask.getWork().getPartDir().size(), ""
              + fTask.getWork().getTblDesc().getTableName(), "" + scanLimit);
        }
      } else {
        // At this point we've run the partition pruner for all top ops. Let's
        // check whether any of them break the limit
        for (Operator<?> topOp : topOps.values()) {
          if (topOp instanceof TableScanOperator) {
            if (((TableScanDesc)topOp.getConf()).getIsMetadataOnly()) {
              continue;
            }
            PrunedPartitionList parts = pCtx.getOpToPartList().get(topOp);
            if (parts.getPartitions().size() > scanLimit) {
              throw new SemanticException(ErrorMsg.PARTITION_SCAN_LIMIT_EXCEEDED, ""
                  + parts.getPartitions().size(), "" + parts.getSourceTable().getTableName(), ""
                  + scanLimit);
            }
          }
        }
      }
    }
  }

  @Override
  public List<FieldSchema> getResultSchema() {
    return resultSchema;
  }

  private void saveViewDefinition() throws SemanticException {

    // Make a copy of the statement's result schema, since we may
    // modify it below as part of imposing view column names.
    List<FieldSchema> derivedSchema =
        new ArrayList<FieldSchema>(resultSchema);
    ParseUtils.validateColumnNameUniqueness(derivedSchema);

    List<FieldSchema> imposedSchema = createVwDesc.getSchema();
    if (imposedSchema != null) {
      int explicitColCount = imposedSchema.size();
      int derivedColCount = derivedSchema.size();
      if (explicitColCount != derivedColCount) {
        throw new SemanticException(generateErrorMessage(
            viewSelect,
            ErrorMsg.VIEW_COL_MISMATCH.getMsg()));
      }
    }

    // Preserve the original view definition as specified by the user.
    String originalText = ctx.getTokenRewriteStream().toString(
        viewSelect.getTokenStartIndex(), viewSelect.getTokenStopIndex());
    createVwDesc.setViewOriginalText(originalText);

    // Now expand the view definition with extras such as explicit column
    // references; this expanded form is what we'll re-parse when the view is
    // referenced later.
    unparseTranslator.applyTranslations(ctx.getTokenRewriteStream());
    String expandedText = ctx.getTokenRewriteStream().toString(
        viewSelect.getTokenStartIndex(), viewSelect.getTokenStopIndex());

    if (imposedSchema != null) {
      // Merge the names from the imposed schema into the types
      // from the derived schema.
      StringBuilder sb = new StringBuilder();
      sb.append("SELECT ");
      int n = derivedSchema.size();
      for (int i = 0; i < n; ++i) {
        if (i > 0) {
          sb.append(", ");
        }
        FieldSchema fieldSchema = derivedSchema.get(i);
        // Modify a copy, not the original
        fieldSchema = new FieldSchema(fieldSchema);
        derivedSchema.set(i, fieldSchema);
        sb.append(HiveUtils.unparseIdentifier(fieldSchema.getName(), conf));
        sb.append(" AS ");
        String imposedName = imposedSchema.get(i).getName();
        sb.append(HiveUtils.unparseIdentifier(imposedName, conf));
        fieldSchema.setName(imposedName);
        // We don't currently allow imposition of a type
        fieldSchema.setComment(imposedSchema.get(i).getComment());
      }
      sb.append(" FROM (");
      sb.append(expandedText);
      sb.append(") ");
      sb.append(HiveUtils.unparseIdentifier(createVwDesc.getViewName(), conf));
      expandedText = sb.toString();
    }

    if (createVwDesc.getPartColNames() != null) {
      // Make sure all partitioning columns referenced actually
      // exist and are in the correct order at the end
      // of the list of columns produced by the view. Also move the field
      // schema descriptors from derivedSchema to the partitioning key
      // descriptor.
      List<String> partColNames = createVwDesc.getPartColNames();
      if (partColNames.size() > derivedSchema.size()) {
        throw new SemanticException(
            ErrorMsg.VIEW_PARTITION_MISMATCH.getMsg());
      }

      // Get the partition columns from the end of derivedSchema.
      List<FieldSchema> partitionColumns = derivedSchema.subList(
          derivedSchema.size() - partColNames.size(),
          derivedSchema.size());

      // Verify that the names match the PARTITIONED ON clause.
      Iterator<String> colNameIter = partColNames.iterator();
      Iterator<FieldSchema> schemaIter = partitionColumns.iterator();
      while (colNameIter.hasNext()) {
        String colName = colNameIter.next();
        FieldSchema fieldSchema = schemaIter.next();
        if (!fieldSchema.getName().equals(colName)) {
          throw new SemanticException(
              ErrorMsg.VIEW_PARTITION_MISMATCH.getMsg());
        }
      }

      // Boundary case: require at least one non-partitioned column
      // for consistency with tables.
      if (partColNames.size() == derivedSchema.size()) {
        throw new SemanticException(
            ErrorMsg.VIEW_PARTITION_TOTAL.getMsg());
      }

      // Now make a copy.
      createVwDesc.setPartCols(
          new ArrayList<FieldSchema>(partitionColumns));

      // Finally, remove the partition columns from the end of derivedSchema.
      // (Clearing the subList writes through to the underlying
      // derivedSchema ArrayList.)
      partitionColumns.clear();
    }

    createVwDesc.setSchema(derivedSchema);
    createVwDesc.setViewExpandedText(expandedText);
  }

  static List<FieldSchema> convertRowSchemaToViewSchema(RowResolver rr) throws SemanticException {
    List<FieldSchema> fieldSchema = convertRowSchemaToResultSetSchema(rr, false);
    ParseUtils.validateColumnNameUniqueness(fieldSchema);
    return fieldSchema;
  }

  static List<FieldSchema> convertRowSchemaToResultSetSchema(RowResolver rr,
      boolean useTabAliasIfAvailable) {
    List<FieldSchema> fieldSchemas = new ArrayList<FieldSchema>();
    String[] qualifiedColName;
    String colName;

    for (ColumnInfo colInfo : rr.getColumnInfos()) {
      if (colInfo.isHiddenVirtualCol()) {
        continue;
      }

      qualifiedColName = rr.reverseLookup(colInfo.getInternalName());
      if (useTabAliasIfAvailable && qualifiedColName[0] != null && !qualifiedColName[0].isEmpty()) {
        colName = qualifiedColName[0] + "." + qualifiedColName[1];
      } else {
        colName = qualifiedColName[1];
      }
      fieldSchemas.add(new FieldSchema(colName, colInfo.getType().getTypeName(), null));
    }
    return fieldSchemas;
  }

  /**
   * Generates an expression node descriptor for the expression with TypeCheckCtx.
   */
  public ExprNodeDesc genExprNodeDesc(ASTNode expr, RowResolver input)
      throws SemanticException {
    // Since the user didn't supply a customized type-checking context,
    // use default settings.
    TypeCheckCtx tcCtx = new TypeCheckCtx(input);
    return genExprNodeDesc(expr, input, tcCtx);
  }

  /**
   * Generates an expression node descriptors for the expression and children of it
   * with default TypeCheckCtx.
   */
  public Map<ASTNode, ExprNodeDesc> genAllExprNodeDesc(ASTNode expr, RowResolver input)
      throws SemanticException {
    TypeCheckCtx tcCtx = new TypeCheckCtx(input);
    return genAllExprNodeDesc(expr, input, tcCtx);
  }

  /**
   * Returns expression node descriptor for the expression.
   * If it's evaluated already in previous operator, it can be retrieved from cache.
   */
  public ExprNodeDesc genExprNodeDesc(ASTNode expr, RowResolver input,
      TypeCheckCtx tcCtx) throws SemanticException {
    // We recursively create the exprNodeDesc. Base cases: when we encounter
    // a column ref, we convert that into an exprNodeColumnDesc; when we
    // encounter
    // a constant, we convert that into an exprNodeConstantDesc. For others we
    // just
    // build the exprNodeFuncDesc with recursively built children.

    // If the current subExpression is pre-calculated, as in Group-By etc.
    ExprNodeDesc cached = getExprNodeDescCached(expr, input);
    if (cached == null) {
      Map<ASTNode, ExprNodeDesc> allExprs = genAllExprNodeDesc(expr, input, tcCtx);
      return allExprs.get(expr);
    }
    return cached;
  }

  /**
   * Find ExprNodeDesc for the expression cached in the RowResolver. Returns null if not exists.
   */
  private ExprNodeDesc getExprNodeDescCached(ASTNode expr, RowResolver input)
      throws SemanticException {
    ColumnInfo colInfo = input.getExpression(expr);
    if (colInfo != null) {
      ASTNode source = input.getExpressionSource(expr);
      if (source != null) {
        unparseTranslator.addCopyTranslation(expr, source);
      }
      return new ExprNodeColumnDesc(colInfo.getType(), colInfo
          .getInternalName(), colInfo.getTabAlias(), colInfo
          .getIsVirtualCol(), colInfo.isSkewedCol());
    }
    return null;
  }

  /**
   * Generates all of the expression node descriptors for the expression and children of it
   * passed in the arguments. This function uses the row resolver and the metadata information
   * that are passed as arguments to resolve the column names to internal names.
   *
   * @param expr
   *          The expression
   * @param input
   *          The row resolver
   * @param tcCtx
   *          Customized type-checking context
   * @return expression to exprNodeDesc mapping
   * @throws SemanticException Failed to evaluate expression
   */
  @SuppressWarnings("nls")
  public Map<ASTNode, ExprNodeDesc> genAllExprNodeDesc(ASTNode expr, RowResolver input,
    TypeCheckCtx tcCtx) throws SemanticException {
    // Create the walker and  the rules dispatcher.
    tcCtx.setUnparseTranslator(unparseTranslator);

    Map<ASTNode, ExprNodeDesc> nodeOutputs =
        TypeCheckProcFactory.genExprNode(expr, tcCtx);
    ExprNodeDesc desc = nodeOutputs.get(expr);
    if (desc == null) {
      String errMsg = tcCtx.getError();
      if (errMsg == null) {
        errMsg = "Error in parsing ";
      }
      throw new SemanticException(errMsg);
    }
    if (desc instanceof ExprNodeColumnListDesc) {
      throw new SemanticException("TOK_ALLCOLREF is not supported in current context");
    }

    if (!unparseTranslator.isEnabled()) {
      // Not creating a view, so no need to track view expansions.
      return nodeOutputs;
    }

    for (Map.Entry<ASTNode, ExprNodeDesc> entry : nodeOutputs.entrySet()) {
      if (!(entry.getValue() instanceof ExprNodeColumnDesc)) {
        continue;
      }
      ASTNode node = entry.getKey();
      ExprNodeColumnDesc columnDesc = (ExprNodeColumnDesc) entry.getValue();
      if ((columnDesc.getTabAlias() == null)
          || (columnDesc.getTabAlias().length() == 0)) {
        // These aren't real column refs; instead, they are special
        // internal expressions used in the representation of aggregation.
        continue;
      }
      String[] tmp = input.reverseLookup(columnDesc.getColumn());
      StringBuilder replacementText = new StringBuilder();
      replacementText.append(HiveUtils.unparseIdentifier(tmp[0], conf));
      replacementText.append(".");
      replacementText.append(HiveUtils.unparseIdentifier(tmp[1], conf));
      unparseTranslator.addTranslation(node, replacementText.toString());
    }

    return nodeOutputs;
  }

  @Override
  public void validate() throws SemanticException {
    LOG.debug("validation start");
    // Validate inputs and outputs have right protectmode to execute the query
    for (ReadEntity readEntity : getInputs()) {
      ReadEntity.Type type = readEntity.getType();

      if (type != ReadEntity.Type.TABLE &&
          type != ReadEntity.Type.PARTITION) {
        // In current implementation it will never happen, but we leave it
        // here to make the logic complete.
        continue;
      }

      Table tbl = readEntity.getTable();
      Partition p = readEntity.getPartition();


      if (tbl.isOffline()) {
        throw new SemanticException(
            ErrorMsg.OFFLINE_TABLE_OR_PARTITION.getMsg(
                "Table " + tbl.getTableName()));
      }

      if (type == ReadEntity.Type.PARTITION && p != null && p.isOffline()) {
        throw new SemanticException(
            ErrorMsg.OFFLINE_TABLE_OR_PARTITION.getMsg(
                "Table " + tbl.getTableName() +
                    " Partition " + p.getName()));
      }
    }

    for (WriteEntity writeEntity : getOutputs()) {
      WriteEntity.Type type = writeEntity.getType();


      if (type == WriteEntity.Type.PARTITION || type == WriteEntity.Type.DUMMYPARTITION) {
        String conflictingArchive;
        try {
          Partition usedp = writeEntity.getPartition();
          Table tbl = usedp.getTable();

          LOG.debug("validated " + usedp.getName());
          LOG.debug(usedp.getTable());
          conflictingArchive = ArchiveUtils
              .conflictingArchiveNameOrNull(db, tbl, usedp.getSpec());
        } catch (HiveException e) {
          throw new SemanticException(e);
        }
        if (conflictingArchive != null) {
          String message = String.format("Insert conflict with existing archive: %s",
              conflictingArchive);
          throw new SemanticException(message);
        }
      }

      if (type != WriteEntity.Type.TABLE &&
          type != WriteEntity.Type.PARTITION) {
        LOG.debug("not validating writeEntity, because entity is neither table nor partition");
        continue;
      }

      Table tbl;
      Partition p;


      if (type == WriteEntity.Type.PARTITION) {
        Partition inputPartition = writeEntity.getPartition();

        // If it is a partition, Partition's metastore is not fetched. We
        // need to fetch it.
        try {
          p = Hive.get().getPartition(
              inputPartition.getTable(), inputPartition.getSpec(), false);
          if (p != null) {
            tbl = p.getTable();
          } else {
            // if p is null, we assume that we insert to a new partition
            tbl = inputPartition.getTable();
          }
        } catch (HiveException e) {
          throw new SemanticException(e);
        }

        if (type == WriteEntity.Type.PARTITION && p != null && p.isOffline()) {
          throw new SemanticException(
              ErrorMsg.OFFLINE_TABLE_OR_PARTITION.getMsg(
                  " Table " + tbl.getTableName() +
                      " Partition " + p.getName()));
        }

      }
      else {
        LOG.debug("Not a partition.");
        tbl = writeEntity.getTable();
      }

      if (tbl.isOffline()) {
        throw new SemanticException(
            ErrorMsg.OFFLINE_TABLE_OR_PARTITION.getMsg(
                "Table " + tbl.getTableName()));
      }
    }

    boolean reworkMapredWork = HiveConf.getBoolVar(this.conf,
        HiveConf.ConfVars.HIVE_REWORK_MAPREDWORK);

    // validate all tasks
    for (Task<? extends Serializable> rootTask : rootTasks) {
      validate(rootTask, reworkMapredWork);
    }
  }

  private void validate(Task<? extends Serializable> task, boolean reworkMapredWork)
      throws SemanticException {
    Utilities.reworkMapRedWork(task, reworkMapredWork, conf);
    if (task.getChildTasks() == null) {
      return;
    }

    for (Task<? extends Serializable> childTask : task.getChildTasks()) {
      validate(childTask, reworkMapredWork);
    }
  }

  /**
   * Get the row resolver given an operator.
   */
  public RowResolver getRowResolver(Operator opt) {
    return opParseCtx.get(opt).getRowResolver();
  }

  /**
   * Add default properties for table property. If a default parameter exists
   * in the tblProp, the value in tblProp will be kept.
   *
   * @param table
   *          property map
   * @return Modified table property map
   */
  private Map<String, String> addDefaultProperties(Map<String, String> tblProp) {
    Map<String, String> retValue;
    if (tblProp == null) {
      retValue = new HashMap<String, String>();
    } else {
      retValue = tblProp;
    }
    String paraString = HiveConf.getVar(conf, ConfVars.NEWTABLEDEFAULTPARA);
    if (paraString != null && !paraString.isEmpty()) {
      for (String keyValuePair : paraString.split(",")) {
        String[] keyValue = keyValuePair.split("=", 2);
        if (keyValue.length != 2) {
          continue;
        }
        if (!retValue.containsKey(keyValue[0])) {
          retValue.put(keyValue[0], keyValue[1]);
        }
      }
    }
    return retValue;
  }

  /**
   * Analyze the create table command. If it is a regular create-table or
   * create-table-like statements, we create a DDLWork and return true. If it is
   * a create-table-as-select, we get the necessary info such as the SerDe and
   * Storage Format and put it in QB, and return false, indicating the rest of
   * the semantic analyzer need to deal with the select statement with respect
   * to the SerDe and Storage Format.
   */
  ASTNode analyzeCreateTable(
      ASTNode ast, QB qb, PlannerContext plannerCtx) throws SemanticException {
    String[] qualifiedTabName = getQualifiedTableName((ASTNode) ast.getChild(0));
    String dbDotTab = getDotName(qualifiedTabName);

    String likeTableName = null;
    List<FieldSchema> cols = new ArrayList<FieldSchema>();
    List<FieldSchema> partCols = new ArrayList<FieldSchema>();
    List<String> bucketCols = new ArrayList<String>();
    List<Order> sortCols = new ArrayList<Order>();
    int numBuckets = -1;
    String comment = null;
    String location = null;
    Map<String, String> tblProps = null;
    boolean ifNotExists = false;
    boolean isExt = false;
    boolean isTemporary = false;
    ASTNode selectStmt = null;
    final int CREATE_TABLE = 0; // regular CREATE TABLE
    final int CTLT = 1; // CREATE TABLE LIKE ... (CTLT)
    final int CTAS = 2; // CREATE TABLE AS SELECT ... (CTAS)
    int command_type = CREATE_TABLE;
    List<String> skewedColNames = new ArrayList<String>();
    List<List<String>> skewedValues = new ArrayList<List<String>>();
    Map<List<String>, String> listBucketColValuesMapping = new HashMap<List<String>, String>();
    boolean storedAsDirs = false;
    boolean isUserStorageFormat = false;

    RowFormatParams rowFormatParams = new RowFormatParams();
    StorageFormat storageFormat = new StorageFormat(conf);

    LOG.info("Creating table " + dbDotTab + " position="
        + ast.getCharPositionInLine());
    int numCh = ast.getChildCount();

    /*
     * Check the 1st-level children and do simple semantic checks: 1) CTLT and
     * CTAS should not coexists. 2) CTLT or CTAS should not coexists with column
     * list (target table schema). 3) CTAS does not support partitioning (for
     * now).
     */
    for (int num = 1; num < numCh; num++) {
      ASTNode child = (ASTNode) ast.getChild(num);
      if (storageFormat.fillStorageFormat(child)) {
        isUserStorageFormat = true;
        continue;
      }
      switch (child.getToken().getType()) {
      case HiveParser.TOK_IFNOTEXISTS:
        ifNotExists = true;
        break;
      case HiveParser.KW_EXTERNAL:
        isExt = true;
        break;
      case HiveParser.KW_TEMPORARY:
        isTemporary = true;
        break;
      case HiveParser.TOK_LIKETABLE:
        if (child.getChildCount() > 0) {
          likeTableName = getUnescapedName((ASTNode) child.getChild(0));
          if (likeTableName != null) {
            if (command_type == CTAS) {
              throw new SemanticException(ErrorMsg.CTAS_CTLT_COEXISTENCE
                  .getMsg());
            }
            if (cols.size() != 0) {
              throw new SemanticException(ErrorMsg.CTLT_COLLST_COEXISTENCE
                  .getMsg());
            }
          }
          command_type = CTLT;
        }
        break;
      case HiveParser.TOK_QUERY: // CTAS
        if (command_type == CTLT) {
          throw new SemanticException(ErrorMsg.CTAS_CTLT_COEXISTENCE.getMsg());
        }
        if (cols.size() != 0) {
          throw new SemanticException(ErrorMsg.CTAS_COLLST_COEXISTENCE.getMsg());
        }
        if (partCols.size() != 0 || bucketCols.size() != 0) {
          boolean dynPart = HiveConf.getBoolVar(conf, HiveConf.ConfVars.DYNAMICPARTITIONING);
          if (dynPart == false) {
            throw new SemanticException(ErrorMsg.CTAS_PARCOL_COEXISTENCE.getMsg());
          } else {
            // TODO: support dynamic partition for CTAS
            throw new SemanticException(ErrorMsg.CTAS_PARCOL_COEXISTENCE.getMsg());
          }
        }
        if (isExt) {
          throw new SemanticException(ErrorMsg.CTAS_EXTTBL_COEXISTENCE.getMsg());
        }
        command_type = CTAS;
        if (plannerCtx != null) {
          plannerCtx.setCTASToken(child);
        }
        selectStmt = child;
        break;
      case HiveParser.TOK_TABCOLLIST:
        cols = getColumns(child);
        break;
      case HiveParser.TOK_TABLECOMMENT:
        comment = unescapeSQLString(child.getChild(0).getText());
        break;
      case HiveParser.TOK_TABLEPARTCOLS:
        partCols = getColumns((ASTNode) child.getChild(0), false);
        break;
      case HiveParser.TOK_ALTERTABLE_BUCKETS:
        bucketCols = getColumnNames((ASTNode) child.getChild(0));
        if (child.getChildCount() == 2) {
          numBuckets = (Integer.valueOf(child.getChild(1).getText()))
              .intValue();
        } else {
          sortCols = getColumnNamesOrder((ASTNode) child.getChild(1));
          numBuckets = (Integer.valueOf(child.getChild(2).getText()))
              .intValue();
        }
        break;
      case HiveParser.TOK_TABLEROWFORMAT:
        rowFormatParams.analyzeRowFormat(child);
        break;
      case HiveParser.TOK_TABLELOCATION:
        location = unescapeSQLString(child.getChild(0).getText());
        location = EximUtil.relativeToAbsolutePath(conf, location);
        inputs.add(toReadEntity(location));
        break;
      case HiveParser.TOK_TABLEPROPERTIES:
        tblProps = DDLSemanticAnalyzer.getProps((ASTNode) child.getChild(0));
        break;
      case HiveParser.TOK_TABLESERIALIZER:
        child = (ASTNode) child.getChild(0);
        storageFormat.setSerde(unescapeSQLString(child.getChild(0).getText()));
        if (child.getChildCount() == 2) {
          readProps((ASTNode) (child.getChild(1).getChild(0)),
              storageFormat.getSerdeProps());
        }
        break;
      case HiveParser.TOK_TABLESKEWED:
        /**
         * Throw an error if the user tries to use the DDL with
         * hive.internal.ddl.list.bucketing.enable set to false.
         */
        HiveConf hiveConf = SessionState.get().getConf();

        // skewed column names
        skewedColNames = analyzeSkewedTablDDLColNames(skewedColNames, child);
        // skewed value
        analyzeDDLSkewedValues(skewedValues, child);
        // stored as directories
        storedAsDirs = analyzeStoredAdDirs(child);

        break;
      default:
        throw new AssertionError("Unknown token: " + child.getToken());
      }
    }

    storageFormat.fillDefaultStorageFormat();

    if ((command_type == CTAS) && (storageFormat.getStorageHandler() != null)) {
      throw new SemanticException(ErrorMsg.CREATE_NON_NATIVE_AS.getMsg());
    }

    // check for existence of table
    if (ifNotExists) {
      try {
        Table table = getTable(qualifiedTabName, false);
        if (table != null) { // table exists
          return null;
        }
      } catch (HiveException e) {
        // should not occur since second parameter to getTableWithQN is false
        throw new IllegalStateException("Unxpected Exception thrown: " + e.getMessage(), e);
      }
    }

    addDbAndTabToOutputs(qualifiedTabName, TableType.MANAGED_TABLE);

    if (isTemporary) {
      if (partCols.size() > 0) {
        throw new SemanticException("Partition columns are not supported on temporary tables");
      }

      if (location == null) {
        // for temporary tables we set the location to something in the session's scratch dir
        // it has the same life cycle as the tmp table
        try {
          // Generate a unique ID for temp table path.
          // This path will be fixed for the life of the temp table.
          Path path = new Path(SessionState.getTempTableSpace(conf), UUID.randomUUID().toString());
          path = Warehouse.getDnsPath(path, conf);
          location = path.toString();
        } catch (MetaException err) {
          throw new SemanticException("Error while generating temp table path:", err);
        }
      }
    }

    // Handle different types of CREATE TABLE command
    CreateTableDesc crtTblDesc = null;
    switch (command_type) {

    case CREATE_TABLE: // REGULAR CREATE TABLE DDL
      tblProps = addDefaultProperties(tblProps);

      crtTblDesc = new CreateTableDesc(dbDotTab, isExt, isTemporary, cols, partCols,
          bucketCols, sortCols, numBuckets, rowFormatParams.fieldDelim,
          rowFormatParams.fieldEscape,
          rowFormatParams.collItemDelim, rowFormatParams.mapKeyDelim, rowFormatParams.lineDelim,
          comment,
          storageFormat.getInputFormat(), storageFormat.getOutputFormat(), location, storageFormat.getSerde(),
          storageFormat.getStorageHandler(), storageFormat.getSerdeProps(), tblProps, ifNotExists, skewedColNames,
          skewedValues);
      crtTblDesc.setStoredAsSubDirectories(storedAsDirs);
      crtTblDesc.setNullFormat(rowFormatParams.nullFormat);

      crtTblDesc.validate(conf);
      // outputs is empty, which means this create table happens in the current
      // database.
      SessionState.get().setCommandType(HiveOperation.CREATETABLE);
      rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
          crtTblDesc), conf));
      break;

    case CTLT: // create table like <tbl_name>
      tblProps = addDefaultProperties(tblProps);

      if (isTemporary) {
        Table likeTable = getTable(likeTableName, false);
        if (likeTable != null && likeTable.getPartCols().size() > 0) {
          throw new SemanticException("Partition columns are not supported on temporary tables "
              + "and source table in CREATE TABLE LIKE is partitioned.");
        }
      }
      CreateTableLikeDesc crtTblLikeDesc = new CreateTableLikeDesc(dbDotTab, isExt, isTemporary,
          storageFormat.getInputFormat(), storageFormat.getOutputFormat(), location,
          storageFormat.getSerde(), storageFormat.getSerdeProps(), tblProps, ifNotExists,
          likeTableName, isUserStorageFormat);
      SessionState.get().setCommandType(HiveOperation.CREATETABLE);
      rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
          crtTblLikeDesc), conf));
      break;

    case CTAS: // create table as select

      // Verify that the table does not already exist
      try {
        Table dumpTable = db.newTable(dbDotTab);
        if (null != db.getTable(dumpTable.getDbName(), dumpTable.getTableName(), false)) {
          throw new SemanticException(ErrorMsg.TABLE_ALREADY_EXISTS.getMsg(dbDotTab));
        }
      } catch (HiveException e) {
        throw new SemanticException(e);
      }

      tblProps = addDefaultProperties(tblProps);

      crtTblDesc = new CreateTableDesc(qualifiedTabName[0], dbDotTab, isExt, isTemporary, cols,
          partCols, bucketCols, sortCols, numBuckets, rowFormatParams.fieldDelim,
          rowFormatParams.fieldEscape, rowFormatParams.collItemDelim, rowFormatParams.mapKeyDelim,
          rowFormatParams.lineDelim, comment, storageFormat.getInputFormat(),
          storageFormat.getOutputFormat(), location, storageFormat.getSerde(),
          storageFormat.getStorageHandler(), storageFormat.getSerdeProps(), tblProps, ifNotExists,
          skewedColNames, skewedValues);
      crtTblDesc.setStoredAsSubDirectories(storedAsDirs);
      crtTblDesc.setNullFormat(rowFormatParams.nullFormat);
      qb.setTableDesc(crtTblDesc);

      SessionState.get().setCommandType(HiveOperation.CREATETABLE_AS_SELECT);

      return selectStmt;
    default:
      throw new SemanticException("Unrecognized command.");
    }
    return null;
  }

  private void addDbAndTabToOutputs(String[] qualifiedTabName, TableType type) throws SemanticException {
    Database database  = getDatabase(qualifiedTabName[0]);
    outputs.add(new WriteEntity(database, WriteEntity.WriteType.DDL_SHARED));

    Table t = new Table(qualifiedTabName[0], qualifiedTabName[1]);
    t.setTableType(type);
    outputs.add(new WriteEntity(t, WriteEntity.WriteType.DDL_NO_LOCK));
  }

  private ASTNode analyzeCreateView(ASTNode ast, QB qb)
      throws SemanticException {
    String[] qualTabName = getQualifiedTableName((ASTNode) ast.getChild(0));
    String dbDotTable = getDotName(qualTabName);
    List<FieldSchema> cols = null;
    boolean ifNotExists = false;
    boolean orReplace = false;
    boolean isAlterViewAs = false;
    String comment = null;
    ASTNode selectStmt = null;
    Map<String, String> tblProps = null;
    List<String> partColNames = null;

    LOG.info("Creating view " + dbDotTable + " position="
        + ast.getCharPositionInLine());
    int numCh = ast.getChildCount();
    for (int num = 1; num < numCh; num++) {
      ASTNode child = (ASTNode) ast.getChild(num);
      switch (child.getToken().getType()) {
      case HiveParser.TOK_IFNOTEXISTS:
        ifNotExists = true;
        break;
      case HiveParser.TOK_ORREPLACE:
        orReplace = true;
        break;
      case HiveParser.TOK_QUERY:
        selectStmt = child;
        break;
      case HiveParser.TOK_TABCOLNAME:
        cols = getColumns(child);
        break;
      case HiveParser.TOK_TABLECOMMENT:
        comment = unescapeSQLString(child.getChild(0).getText());
        break;
      case HiveParser.TOK_TABLEPROPERTIES:
        tblProps = DDLSemanticAnalyzer.getProps((ASTNode) child.getChild(0));
        break;
      case HiveParser.TOK_VIEWPARTCOLS:
        partColNames = getColumnNames((ASTNode) child.getChild(0));
        break;
      default:
        assert false;
      }
    }

    if (ifNotExists && orReplace){
      throw new SemanticException("Can't combine IF NOT EXISTS and OR REPLACE.");
    }

    if (ast.getToken().getType() == HiveParser.TOK_ALTERVIEW &&
        ast.getChild(1).getType() == HiveParser.TOK_QUERY) {
      isAlterViewAs = true;
      orReplace = true;
    }

    createVwDesc = new CreateViewDesc(
      dbDotTable, cols, comment, tblProps, partColNames,
      ifNotExists, orReplace, isAlterViewAs);

    unparseTranslator.enable();
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
        createVwDesc), conf));

    addDbAndTabToOutputs(qualTabName, TableType.VIRTUAL_VIEW);
    return selectStmt;
  }

  CreateViewDesc getCreateViewDesc() {
    return this.createVwDesc;
  }

  // validate the create view statement
  // the statement could be CREATE VIEW, REPLACE VIEW, or ALTER VIEW AS SELECT
  // check semantic conditions
  private void validateCreateView(CreateViewDesc createVwDesc)
    throws SemanticException {
    try {
      Table oldView = getTable(createVwDesc.getViewName(), false);

      // Do not allow view to be defined on temp table
      Set<String> tableAliases = qb.getTabAliases();
      for (String alias : tableAliases) {
        try {
          Table table = db.getTable(qb.getTabNameForAlias(alias));
          if (table.isTemporary()) {
            throw new SemanticException("View definition references temporary table " + alias);
          }
        } catch (HiveException ex) {
          throw new SemanticException(ex);
        }
      }

      // ALTER VIEW AS SELECT requires the view must exist
      if (createVwDesc.getIsAlterViewAs() && oldView == null) {
        String viewNotExistErrorMsg =
          "The following view does not exist: " + createVwDesc.getViewName();
        throw new SemanticException(
          ErrorMsg.ALTER_VIEW_AS_SELECT_NOT_EXIST.getMsg(viewNotExistErrorMsg));
      }

      //replace view
      if (createVwDesc.getOrReplace() && oldView != null) {

        // Existing table is not a view
        if (!oldView.getTableType().equals(TableType.VIRTUAL_VIEW)) {
          String tableNotViewErrorMsg =
            "The following is an existing table, not a view: " +
            createVwDesc.getViewName();
          throw new SemanticException(
            ErrorMsg.EXISTING_TABLE_IS_NOT_VIEW.getMsg(tableNotViewErrorMsg));
        }

        // if old view has partitions, it could not be replaced
        String partitionViewErrorMsg =
          "The following view has partition, it could not be replaced: " +
          createVwDesc.getViewName();
        try {
          if ((createVwDesc.getPartCols() == null ||
            createVwDesc.getPartCols().isEmpty() ||
            !createVwDesc.getPartCols().equals(oldView.getPartCols())) &&
            !oldView.getPartCols().isEmpty() &&
            !db.getPartitions(oldView).isEmpty()) {
            throw new SemanticException(
              ErrorMsg.REPLACE_VIEW_WITH_PARTITION.getMsg(partitionViewErrorMsg));
          }
        } catch (HiveException e) {
          throw new SemanticException(
            ErrorMsg.REPLACE_VIEW_WITH_PARTITION.getMsg(partitionViewErrorMsg));
        }
      }
    } catch (HiveException e) {
      throw new SemanticException(e.getMessage());
    }
  }

  // Process the position alias in GROUPBY and ORDERBY
  private void processPositionAlias(ASTNode ast) throws SemanticException {
    if (HiveConf.getBoolVar(conf,
          HiveConf.ConfVars.HIVE_GROUPBY_ORDERBY_POSITION_ALIAS) == false) {
      return;
    }

    if (ast.getChildCount()  == 0) {
      return;
    }

    boolean isAllCol;
    ASTNode selectNode = null;
    ASTNode groupbyNode = null;
    ASTNode orderbyNode = null;

    // get node type
    int child_count = ast.getChildCount();
    for (int child_pos = 0; child_pos < child_count; ++child_pos) {
      ASTNode node = (ASTNode) ast.getChild(child_pos);
      int type = node.getToken().getType();
      if (type == HiveParser.TOK_SELECT) {
        selectNode = node;
      } else if (type == HiveParser.TOK_GROUPBY) {
        groupbyNode = node;
      } else if (type == HiveParser.TOK_ORDERBY) {
        orderbyNode = node;
      }
    }

    if (selectNode != null) {
      int selectExpCnt = selectNode.getChildCount();

      // replace each of the position alias in GROUPBY with the actual column name
      if (groupbyNode != null) {
        for (int child_pos = 0; child_pos < groupbyNode.getChildCount(); ++child_pos) {
          ASTNode node = (ASTNode) groupbyNode.getChild(child_pos);
          if (node.getToken().getType() == HiveParser.Number) {
            int pos = Integer.parseInt(node.getText());
            if (pos > 0 && pos <= selectExpCnt) {
              groupbyNode.setChild(child_pos,
                selectNode.getChild(pos - 1).getChild(0));
            } else {
              throw new SemanticException(
                ErrorMsg.INVALID_POSITION_ALIAS_IN_GROUPBY.getMsg(
                "Position alias: " + pos + " does not exist\n" +
                "The Select List is indexed from 1 to " + selectExpCnt));
            }
          }
        }
      }

      // replace each of the position alias in ORDERBY with the actual column name
      if (orderbyNode != null) {
        isAllCol = false;
        for (int child_pos = 0; child_pos < selectNode.getChildCount(); ++child_pos) {
          ASTNode node = (ASTNode) selectNode.getChild(child_pos).getChild(0);
          if (node.getToken().getType() == HiveParser.TOK_ALLCOLREF) {
            isAllCol = true;
          }
        }
        for (int child_pos = 0; child_pos < orderbyNode.getChildCount(); ++child_pos) {
          ASTNode colNode = (ASTNode) orderbyNode.getChild(child_pos);
          ASTNode node = (ASTNode) colNode.getChild(0);
          if (node.getToken().getType() == HiveParser.Number) {
            if (!isAllCol) {
              int pos = Integer.parseInt(node.getText());
              if (pos > 0 && pos <= selectExpCnt) {
                colNode.setChild(0, selectNode.getChild(pos - 1).getChild(0));
              } else {
                throw new SemanticException(
                  ErrorMsg.INVALID_POSITION_ALIAS_IN_ORDERBY.getMsg(
                  "Position alias: " + pos + " does not exist\n" +
                  "The Select List is indexed from 1 to " + selectExpCnt));
              }
            } else {
              throw new SemanticException(
                ErrorMsg.NO_SUPPORTED_ORDERBY_ALLCOLREF_POS.getMsg());
            }
          }
        }
      }
    }

    // Recursively process through the children ASTNodes
    for (int child_pos = 0; child_pos < child_count; ++child_pos) {
      processPositionAlias((ASTNode) ast.getChild(child_pos));
    }
    return;
  }

  /**
   * process analyze ... partial command
   *
   * separate it from noscan command process so that it provides us flexibility
   *
   * @param tree
   * @throws SemanticException
   */
  protected void processPartialScanCommand (ASTNode tree) throws SemanticException {
    // check if it is partial scan command
    this.checkPartialScan(tree);

    //validate partial scan
    if (this.partialscan) {
      validateAnalyzePartialscan(tree);
    }
  }

  /**
   * process analyze ... noscan command
   * @param tree
   * @throws SemanticException
   */
  protected void processNoScanCommand (ASTNode tree) throws SemanticException {
    // check if it is noscan command
    checkNoScan(tree);

    //validate noscan
    if (this.noscan) {
      validateAnalyzeNoscan(tree);
    }
  }

  /**
   * Validate noscan command
   *
   * @param tree
   * @throws SemanticException
   */
  private void validateAnalyzeNoscan(ASTNode tree) throws SemanticException {
    // since it is noscan, it is true table name in command
    String tableName = getUnescapedName((ASTNode) tree.getChild(0).getChild(0));
    Table tbl;
    try {
      tbl = db.getTable(tableName);
    } catch (InvalidTableException e) {
      throw new SemanticException(ErrorMsg.INVALID_TABLE.getMsg(tableName), e);
    }
    catch (HiveException e) {
      throw new SemanticException(e.getMessage(), e);
    }

    /* noscan uses hdfs apis to retrieve such information from Namenode.      */
    /* But that will be specific to hdfs. Through storagehandler mechanism,   */
    /* storage of table could be on any storage system: hbase, cassandra etc. */
    /* A nice error message should be given to user. */
    if (tbl.isNonNative()) {
      throw new SemanticException(ErrorMsg.ANALYZE_TABLE_NOSCAN_NON_NATIVE.getMsg(tbl
          .getTableName()));
    }
  }

  /**
   * Validate partialscan command
   *
   * @param tree
   * @throws SemanticException
   */
  private void validateAnalyzePartialscan(ASTNode tree) throws SemanticException {
    // since it is partialscan, it is true table name in command
    String tableName = getUnescapedName((ASTNode) tree.getChild(0).getChild(0));
    Table tbl;
    try {
      tbl = db.getTable(tableName);
    } catch (InvalidTableException e) {
      throw new SemanticException(ErrorMsg.INVALID_TABLE.getMsg(tableName), e);
    } catch (HiveException e) {
      throw new SemanticException(e.getMessage(), e);
    }
    /* partialscan uses hdfs apis to retrieve such information from Namenode.      */
    /* But that will be specific to hdfs. Through storagehandler mechanism,   */
    /* storage of table could be on any storage system: hbase, cassandra etc. */
    /* A nice error message should be given to user. */
    if (tbl.isNonNative()) {
      throw new SemanticException(ErrorMsg.ANALYZE_TABLE_PARTIALSCAN_NON_NATIVE.getMsg(tbl
          .getTableName()));
    }

    /**
     * Partial scan doesn't support external table.
     */
    if(tbl.getTableType().equals(TableType.EXTERNAL_TABLE)) {
      throw new SemanticException(ErrorMsg.ANALYZE_TABLE_PARTIALSCAN_EXTERNAL_TABLE.getMsg(tbl
          .getTableName()));
    }

    if (!HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVESTATSAUTOGATHER)) {
      throw new SemanticException(ErrorMsg.ANALYZE_TABLE_PARTIALSCAN_AUTOGATHER.getMsg());
    }
  }

  /**
   * It will check if this is analyze ... compute statistics noscan
   * @param tree
   */
  private void checkNoScan(ASTNode tree) {
    if (tree.getChildCount() > 1) {
      ASTNode child0 = (ASTNode) tree.getChild(0);
      ASTNode child1;
      if (child0.getToken().getType() == HiveParser.TOK_TAB) {
        child0 = (ASTNode) child0.getChild(0);
        if (child0.getToken().getType() == HiveParser.TOK_TABNAME) {
          child1 = (ASTNode) tree.getChild(1);
          if (child1.getToken().getType() == HiveParser.KW_NOSCAN) {
            this.noscan = true;
          }
        }
      }
    }
  }

  /**
   * It will check if this is analyze ... compute statistics partialscan
   * @param tree
   */
  private void checkPartialScan(ASTNode tree) {
    if (tree.getChildCount() > 1) {
      ASTNode child0 = (ASTNode) tree.getChild(0);
      ASTNode child1;
      if (child0.getToken().getType() == HiveParser.TOK_TAB) {
        child0 = (ASTNode) child0.getChild(0);
        if (child0.getToken().getType() == HiveParser.TOK_TABNAME) {
          child1 = (ASTNode) tree.getChild(1);
          if (child1.getToken().getType() == HiveParser.KW_PARTIALSCAN) {
            this.partialscan = true;
          }
        }
      }
    }
  }


  public QB getQB() {
    return qb;
  }

  public void setQB(QB qb) {
    this.qb = qb;
  }

//--------------------------- PTF handling -----------------------------------

  /*
   * - a partitionTableFunctionSource can be a tableReference, a SubQuery or another
   *   PTF invocation.
   * - For a TABLEREF: set the source to the alias returned by processTable
   * - For a SubQuery: set the source to the alias returned by processSubQuery
   * - For a PTF invocation: recursively call processPTFChain.
   */
  private PTFInputSpec processPTFSource(QB qb, ASTNode inputNode) throws SemanticException{

    PTFInputSpec qInSpec = null;
    int type = inputNode.getType();
    String alias;
    switch(type)
    {
    case HiveParser.TOK_TABREF:
      alias = processTable(qb, inputNode);
      qInSpec = new PTFQueryInputSpec();
      ((PTFQueryInputSpec)qInSpec).setType(PTFQueryInputType.TABLE);
      ((PTFQueryInputSpec)qInSpec).setSource(alias);
      break;
    case HiveParser.TOK_SUBQUERY:
      alias = processSubQuery(qb, inputNode);
      qInSpec = new PTFQueryInputSpec();
      ((PTFQueryInputSpec)qInSpec).setType(PTFQueryInputType.SUBQUERY);
      ((PTFQueryInputSpec)qInSpec).setSource(alias);
      break;
    case HiveParser.TOK_PTBLFUNCTION:
      qInSpec = processPTFChain(qb, inputNode);
      break;
    default:
      throw new SemanticException(generateErrorMessage(inputNode,
          "Unknown input type to PTF"));
    }

    qInSpec.setAstNode(inputNode);
    return qInSpec;

  }

  /*
   * - tree form is
   *   ^(TOK_PTBLFUNCTION name alias? partitionTableFunctionSource partitioningSpec? arguments*)
   * - a partitionTableFunctionSource can be a tableReference, a SubQuery or another
   *   PTF invocation.
   */
  private PartitionedTableFunctionSpec processPTFChain(QB qb, ASTNode ptf)
      throws SemanticException{
    int child_count = ptf.getChildCount();
    if (child_count < 2) {
      throw new SemanticException(generateErrorMessage(ptf,
                  "Not enough Children " + child_count));
    }

    PartitionedTableFunctionSpec ptfSpec = new PartitionedTableFunctionSpec();
    ptfSpec.setAstNode(ptf);

    /*
     * name
     */
    ASTNode nameNode = (ASTNode) ptf.getChild(0);
    ptfSpec.setName(nameNode.getText());

    int inputIdx = 1;

    /*
     * alias
     */
    ASTNode secondChild = (ASTNode) ptf.getChild(1);
    if ( secondChild.getType() == HiveParser.Identifier ) {
      ptfSpec.setAlias(secondChild.getText());
      inputIdx++;
    }

    /*
     * input
     */
    ASTNode inputNode = (ASTNode) ptf.getChild(inputIdx);
    ptfSpec.setInput(processPTFSource(qb, inputNode));

    int argStartIdx = inputIdx + 1;

    /*
     * partitioning Spec
     */
    int pSpecIdx = inputIdx + 1;
    ASTNode pSpecNode = ptf.getChildCount() > inputIdx ?
        (ASTNode) ptf.getChild(pSpecIdx) : null;
    if (pSpecNode != null && pSpecNode.getType() == HiveParser.TOK_PARTITIONINGSPEC)
    {
      PartitioningSpec partitioning = processPTFPartitionSpec(pSpecNode);
      ptfSpec.setPartitioning(partitioning);
      argStartIdx++;
    }

    /*
     * arguments
     */
    for(int i=argStartIdx; i < ptf.getChildCount(); i++)
    {
      ptfSpec.addArg((ASTNode) ptf.getChild(i));
    }
    return ptfSpec;
  }

  /*
   * - invoked during FROM AST tree processing, on encountering a PTF invocation.
   * - tree form is
   *   ^(TOK_PTBLFUNCTION name partitionTableFunctionSource partitioningSpec? arguments*)
   * - setup a PTFInvocationSpec for this top level PTF invocation.
   */
  private void processPTF(QB qb, ASTNode ptf) throws SemanticException{

    PartitionedTableFunctionSpec ptfSpec = processPTFChain(qb, ptf);

    if ( ptfSpec.getAlias() != null ) {
      qb.addAlias(ptfSpec.getAlias());
    }

    PTFInvocationSpec spec = new PTFInvocationSpec();
    spec.setFunction(ptfSpec);
    qb.addPTFNodeToSpec(ptf, spec);
  }

  private void handleQueryWindowClauses(QB qb, Phase1Ctx ctx_1, ASTNode node)
      throws SemanticException {
    WindowingSpec spec = qb.getWindowingSpec(ctx_1.dest);
    for(int i=0; i < node.getChildCount(); i++) {
      processQueryWindowClause(spec, (ASTNode) node.getChild(i));
    }
  }

  private PartitionSpec processPartitionSpec(ASTNode node) {
    PartitionSpec pSpec = new PartitionSpec();
    int exprCnt = node.getChildCount();
    for(int i=0; i < exprCnt; i++) {
      PartitionExpression exprSpec = new PartitionExpression();
      exprSpec.setExpression((ASTNode) node.getChild(i));
      pSpec.addExpression(exprSpec);
    }
    return pSpec;
  }

  private OrderSpec processOrderSpec(ASTNode sortNode) {
    OrderSpec oSpec = new OrderSpec();
    int exprCnt = sortNode.getChildCount();
    for(int i=0; i < exprCnt; i++) {
      OrderExpression exprSpec = new OrderExpression();
      exprSpec.setExpression((ASTNode) sortNode.getChild(i).getChild(0));
      if ( sortNode.getChild(i).getType() == HiveParser.TOK_TABSORTCOLNAMEASC ) {
        exprSpec.setOrder(org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.Order.ASC);
      }
      else {
        exprSpec.setOrder(org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.Order.DESC);
      }
      oSpec.addExpression(exprSpec);
    }
    return oSpec;
  }

  private PartitioningSpec processPTFPartitionSpec(ASTNode pSpecNode)
  {
    PartitioningSpec partitioning = new PartitioningSpec();
    ASTNode firstChild = (ASTNode) pSpecNode.getChild(0);
    int type = firstChild.getType();
    int exprCnt;


    if ( type == HiveParser.TOK_DISTRIBUTEBY || type == HiveParser.TOK_CLUSTERBY )
    {
      PartitionSpec pSpec = processPartitionSpec(firstChild);
      partitioning.setPartSpec(pSpec);
      ASTNode sortNode = pSpecNode.getChildCount() > 1 ? (ASTNode) pSpecNode.getChild(1) : null;
      if ( sortNode != null )
      {
        OrderSpec oSpec = processOrderSpec(sortNode);
        partitioning.setOrderSpec(oSpec);
      }
    }
    else if ( type == HiveParser.TOK_SORTBY || type == HiveParser.TOK_ORDERBY ) {
      ASTNode sortNode = firstChild;
      OrderSpec oSpec = processOrderSpec(sortNode);
      partitioning.setOrderSpec(oSpec);
    }
    return partitioning;
  }

  private WindowFunctionSpec processWindowFunction(ASTNode node, ASTNode wsNode)
    throws SemanticException {
    WindowFunctionSpec wfSpec = new WindowFunctionSpec();

    switch(node.getType()) {
    case HiveParser.TOK_FUNCTIONSTAR:
      wfSpec.setStar(true);
      break;
    case HiveParser.TOK_FUNCTIONDI:
      wfSpec.setDistinct(true);
      break;
    }

    if ( wfSpec.isDistinct() ) {
      throw new SemanticException(generateErrorMessage(node,
          "Count/Sum distinct not supported with Windowing"));
    }

    wfSpec.setExpression(node);

    ASTNode nameNode = (ASTNode) node.getChild(0);
    wfSpec.setName(nameNode.getText());

    for(int i=1; i < node.getChildCount()-1; i++) {
      ASTNode child = (ASTNode) node.getChild(i);
      wfSpec.addArg(child);
    }

    if ( wsNode != null ) {
      WindowSpec ws = processWindowSpec(wsNode);
      wfSpec.setWindowSpec(ws);
    }

    return wfSpec;
  }

  private boolean containsLeadLagUDF(ASTNode expressionTree) {
    int exprTokenType = expressionTree.getToken().getType();
    if (exprTokenType == HiveParser.TOK_FUNCTION) {
      assert (expressionTree.getChildCount() != 0);
      if (expressionTree.getChild(0).getType() == HiveParser.Identifier) {
        String functionName = unescapeIdentifier(expressionTree.getChild(0)
            .getText());
        functionName = functionName.toLowerCase();
        if ( FunctionRegistry.LAG_FUNC_NAME.equals(functionName) ||
            FunctionRegistry.LEAD_FUNC_NAME.equals(functionName)
            ) {
          return true;
        }
      }
    }
    for (int i = 0; i < expressionTree.getChildCount(); i++) {
      if ( containsLeadLagUDF((ASTNode) expressionTree.getChild(i))) {
        return true;
      }
    }
    return false;
  }

  private void processQueryWindowClause(WindowingSpec spec, ASTNode node)
      throws SemanticException {
    ASTNode nameNode = (ASTNode) node.getChild(0);
    ASTNode wsNode = (ASTNode) node.getChild(1);
    if(spec.getWindowSpecs() != null && spec.getWindowSpecs().containsKey(nameNode.getText())){
      throw new SemanticException(generateErrorMessage(nameNode,
          "Duplicate definition of window " + nameNode.getText() +
          " is not allowed"));
    }
    WindowSpec ws = processWindowSpec(wsNode);
    spec.addWindowSpec(nameNode.getText(), ws);
  }

  private WindowSpec processWindowSpec(ASTNode node) throws SemanticException {
    String sourceId = null;
    PartitionSpec partition = null;
    OrderSpec order = null;
    WindowFrameSpec windowFrame = null;

    boolean hasSrcId = false, hasPartSpec = false, hasWF = false;
    int srcIdIdx = -1, partIdx = -1, wfIdx = -1;

    for(int i=0; i < node.getChildCount(); i++)
    {
      int type = node.getChild(i).getType();
      switch(type)
      {
      case HiveParser.Identifier:
        hasSrcId = true; srcIdIdx = i;
        break;
      case HiveParser.TOK_PARTITIONINGSPEC:
        hasPartSpec = true; partIdx = i;
        break;
      case HiveParser.TOK_WINDOWRANGE:
      case HiveParser.TOK_WINDOWVALUES:
        hasWF = true; wfIdx = i;
        break;
      }
    }

    WindowSpec ws = new WindowSpec();

    if (hasSrcId) {
      ASTNode nameNode = (ASTNode) node.getChild(srcIdIdx);
      ws.setSourceId(nameNode.getText());
    }

    if (hasPartSpec) {
      ASTNode partNode = (ASTNode) node.getChild(partIdx);
      PartitioningSpec partitioning = processPTFPartitionSpec(partNode);
      ws.setPartitioning(partitioning);
    }

    if ( hasWF)
    {
      ASTNode wfNode = (ASTNode) node.getChild(wfIdx);
      WindowFrameSpec wfSpec = processWindowFrame(wfNode);
      ws.setWindowFrame(wfSpec);
    }

    return ws;
  }

  private WindowFrameSpec processWindowFrame(ASTNode node) throws SemanticException {
    int type = node.getType();
    BoundarySpec start = null, end = null;

    /*
     * A WindowFrame may contain just the Start Boundary or in the
     * between style of expressing a WindowFrame both boundaries
     * are specified.
     */
    start = processBoundary(type, (ASTNode) node.getChild(0));
    if ( node.getChildCount() > 1 ) {
      end = processBoundary(type, (ASTNode) node.getChild(1));
    }

    return new WindowFrameSpec(start, end);
  }

  private BoundarySpec processBoundary(int frameType, ASTNode node)  throws SemanticException {
    BoundarySpec bs = frameType == HiveParser.TOK_WINDOWRANGE ?
        new RangeBoundarySpec() : new ValueBoundarySpec();
    int type = node.getType();
    boolean hasAmt = true;

    switch(type)
    {
    case HiveParser.KW_PRECEDING:
      bs.setDirection(Direction.PRECEDING);
      break;
    case HiveParser.KW_FOLLOWING:
      bs.setDirection(Direction.FOLLOWING);
      break;
    case HiveParser.KW_CURRENT:
      bs = new CurrentRowSpec();
      hasAmt = false;
      break;
    }

    if ( hasAmt )
    {
      ASTNode amtNode = (ASTNode) node.getChild(0);
      if ( amtNode.getType() == HiveParser.KW_UNBOUNDED)
      {
        bs.setAmt(BoundarySpec.UNBOUNDED_AMOUNT);
      }
      else
      {
        int amt = Integer.parseInt(amtNode.getText());
        if ( amt < 0 ) {
          throw new SemanticException(
              "Window Frame Boundary Amount must be a +ve integer, amount provide is: " + amt);
        }
        bs.setAmt(amt);
      }
    }

    return bs;
  }

  /*
   * check if a Select Expr is a constant.
   * - current logic used is to look for HiveParser.TOK_TABLE_OR_COL
   * - if there is none then the expression is a constant.
   */
  private static class ConstantExprCheck implements ContextVisitor {
    boolean isConstant = true;

    @Override
    public void visit(Object t, Object parent, int childIndex, Map labels) {
      if ( !isConstant ) {
        return;
      }
      ASTNode node = (ASTNode) t;
      if (ParseDriver.adaptor.getType(t) == HiveParser.TOK_TABLE_OR_COL ) {
        isConstant = false;
      }
    }

    public void reset() {
      isConstant = true;
    }

    protected boolean isConstant() {
      return isConstant;
    }
  }

  private static class AggregationExprCheck implements ContextVisitor {
    HashMap<String, ASTNode> destAggrExprs;
    boolean isAggr = false;

    public AggregationExprCheck(HashMap<String, ASTNode> destAggrExprs) {
      super();
      this.destAggrExprs = destAggrExprs;
    }

    @Override
    public void visit(Object t, Object parent, int childIndex, Map labels) {
      if ( isAggr ) {
        return;
      }
      if ( destAggrExprs.values().contains(t)) {
        isAggr = true;
      }
    }

    public void reset() {
      isAggr = false;
    }

    protected boolean isAggr() {
      return isAggr;
    }
  }

  /*
   * Returns false if there is a SelectExpr that is not a constant or an aggr.
   *
   */
  private boolean isValidGroupBySelectList(QB currQB, String clause){
    ConstantExprCheck constantExprCheck = new ConstantExprCheck();
    AggregationExprCheck aggrExprCheck = new AggregationExprCheck(
        currQB.getParseInfo().getAggregationExprsForClause(clause));

    TreeWizard tw = new TreeWizard(ParseDriver.adaptor, HiveParser.tokenNames);
    ASTNode selectNode = currQB.getParseInfo().getSelForClause(clause);

    /*
     * for Select Distinct Queries we don't move any aggregations.
     */
    if ( selectNode != null && selectNode.getType() == HiveParser.TOK_SELECTDI ) {
      return true;
    }

    for (int i = 0; selectNode != null && i < selectNode.getChildCount(); i++) {
      ASTNode selectExpr = (ASTNode) selectNode.getChild(i);
      //check for TOK_HINTLIST expressions on ast
      if(selectExpr.getType() != HiveParser.TOK_SELEXPR){
        continue;
      }

      constantExprCheck.reset();
      PTFTranslator.visit(selectExpr.getChild(0), constantExprCheck);

      if ( !constantExprCheck.isConstant() ) {
        aggrExprCheck.reset();
        PTFTranslator.visit(selectExpr.getChild(0), aggrExprCheck);
        if (!aggrExprCheck.isAggr() ) {
          return false;
        }
      }

    }
    return true;
  }

//--------------------------- PTF handling: PTFInvocationSpec to PTFDesc --------------------------

  private PTFDesc translatePTFInvocationSpec(PTFInvocationSpec ptfQSpec, RowResolver inputRR)
      throws SemanticException{
    PTFDesc ptfDesc = null;
    PTFTranslator translator = new PTFTranslator();
    ptfDesc = translator.translate(ptfQSpec, this, conf, inputRR, unparseTranslator);
    return ptfDesc;
  }

  Operator genPTFPlan(PTFInvocationSpec ptfQSpec, Operator input) throws SemanticException {
    ArrayList<PTFInvocationSpec> componentQueries = PTFTranslator.componentize(ptfQSpec);
    for (PTFInvocationSpec ptfSpec : componentQueries) {
      input = genPTFPlanForComponentQuery(ptfSpec, input);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Created PTF Plan ");
    }
    return input;
  }


  /**
   * Construct the data structures containing ExprNodeDesc for partition
   * columns and order columns. Use the input definition to construct the list
   * of output columns for the ReduceSinkOperator
   *
   * @throws SemanticException
   */
  void buildPTFReduceSinkDetails(PartitionedTableFunctionDef tabDef,
      RowResolver inputRR,
      ArrayList<ExprNodeDesc> partCols,
      ArrayList<ExprNodeDesc> valueCols,
      ArrayList<ExprNodeDesc> orderCols,
      Map<String, ExprNodeDesc> colExprMap,
      List<String> outputColumnNames,
      StringBuilder orderString,
      RowResolver rsOpRR,
      RowResolver extractRR) throws SemanticException {

    List<PTFExpressionDef> partColList = tabDef.getPartition().getExpressions();

    for (PTFExpressionDef colDef : partColList) {
      partCols.add(colDef.getExprNode());
      orderCols.add(colDef.getExprNode());
      orderString.append('+');
    }

    /*
     * Order columns are used as key columns for constructing
     * the ReduceSinkOperator
     * Since we do not explicitly add these to outputColumnNames,
     * we need to set includeKeyCols = false while creating the
     * ReduceSinkDesc
     */
    List<OrderExpressionDef> orderColList = tabDef.getOrder().getExpressions();
    for (int i = 0; i < orderColList.size(); i++) {
      OrderExpressionDef colDef = orderColList.get(i);
      org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.Order order = colDef.getOrder();
      if (order.name().equals("ASC")) {
        orderString.append('+');
      } else {
        orderString.append('-');
      }
      orderCols.add(colDef.getExprNode());
    }

    ArrayList<ColumnInfo> colInfoList = inputRR.getColumnInfos();
    /*
     * construct the ReduceSinkRR
     */
    int pos = 0;
    for (ColumnInfo colInfo : colInfoList) {
        ExprNodeDesc valueColExpr = new ExprNodeColumnDesc(colInfo);
        valueCols.add(valueColExpr);
        String internalName = SemanticAnalyzer.getColumnInternalName(pos++);
        outputColumnNames.add(internalName);
        colExprMap.put(internalName, valueColExpr);

        String[] alias = inputRR.reverseLookup(colInfo.getInternalName());
        ColumnInfo newColInfo = new ColumnInfo(
            internalName, colInfo.getType(), alias[0],
            colInfo.getIsVirtualCol(), colInfo.isHiddenVirtualCol());
        rsOpRR.put(alias[0], alias[1], newColInfo);
    }

    /*
     * construct the ExtractRR
     */
    LinkedHashMap<String[], ColumnInfo> colsAddedByHaving =
        new LinkedHashMap<String[], ColumnInfo>();
    pos = 0;
    for (ColumnInfo colInfo : colInfoList) {
      String[] alias = inputRR.reverseLookup(colInfo.getInternalName());
      /*
       * if we have already encountered this colInfo internalName.
       * We encounter it again because it must be put for the Having clause.
       * We will add these entries in the end; in a loop on colsAddedByHaving. See below.
       */
      if ( colsAddedByHaving.containsKey(alias)) {
        continue;
      }
      ASTNode astNode = PTFTranslator.getASTNode(colInfo, inputRR);
      ColumnInfo eColInfo = new ColumnInfo(
          SemanticAnalyzer.getColumnInternalName(pos++), colInfo.getType(), alias[0],
          colInfo.getIsVirtualCol(), colInfo.isHiddenVirtualCol());

      if ( astNode == null ) {
        extractRR.put(alias[0], alias[1], eColInfo);
      }
      else {
        /*
         * in case having clause refers to this column may have been added twice;
         * once with the ASTNode.toStringTree as the alias
         * and then with the real alias.
         */
        extractRR.putExpression(astNode, eColInfo);
        if ( !astNode.toStringTree().toLowerCase().equals(alias[1]) ) {
          colsAddedByHaving.put(alias, eColInfo);
        }
      }
    }

    for(Map.Entry<String[], ColumnInfo> columnAddedByHaving : colsAddedByHaving.entrySet() ) {
      String[] alias = columnAddedByHaving.getKey();
      ColumnInfo eColInfo = columnAddedByHaving.getValue();
      extractRR.put(alias[0], alias[1], eColInfo);
    }
  }

  private Operator genPTFPlanForComponentQuery(PTFInvocationSpec ptfQSpec, Operator input)
      throws SemanticException {
    /*
     * 1. Create the PTFDesc from the Qspec attached to this QB.
     */
    RowResolver rr = opParseCtx.get(input).getRowResolver();
    PTFDesc ptfDesc = translatePTFInvocationSpec(ptfQSpec, rr);

    RowResolver rsOpRR = new RowResolver();
    /*
     * Build an RR for the Extract Op from the ReduceSink Op's RR.
     * Why?
     * We need to remove the Virtual Columns present in the RS's RR. The OI
     * that gets passed to Extract at runtime doesn't contain the Virtual Columns.
     * So internal names get changed. Consider testCase testJoinWithLeadLag,
     * which is a self join on part and also has a Windowing expression.
     * The RR of the RS op at translation time looks something like this:
     * (_co1,_col2,..,_col7, _col8(vc=true),_col9(vc=true),
     * _col10,_col11,.._col15(vc=true),_col16(vc=true),..)
     * At runtime the Virtual columns are removed and all the columns after _col7
     * are shifted 1 or 2 positions.
     * So in child Operators ColumnExprNodeDesc's are no longer referring to the right columns.
     *
     * So we build a new RR for the Extract Op, with the Virtual Columns removed.
     * We hand this to the PTFTranslator as the
     * starting RR to use to translate a PTF Chain.
     */
    RowResolver extractOpRR = new RowResolver();

    /*
     * 2. build Map-side Op Graph. Graph template is either:
     * Input -> PTF_map -> ReduceSink
     * or
     * Input -> ReduceSink
     *
     * Here the ExprNodeDescriptors in the QueryDef are based on the Input Operator's RR.
     */
    {
      PartitionedTableFunctionDef tabDef = ptfDesc.getStartOfChain();

      /*
       * a. add Map-side PTF Operator if needed
       */
      if (tabDef.isTransformsRawInput() )
      {
        RowResolver ptfMapRR = tabDef.getRawInputShape().getRr();

        ptfDesc.setMapSide(true);
        input = putOpInsertMap(OperatorFactory.getAndMakeChild(ptfDesc,
            new RowSchema(ptfMapRR.getColumnInfos()),
            input), ptfMapRR);
        rr = opParseCtx.get(input).getRowResolver();
      }

      /*
       * b. Build Reduce Sink Details (keyCols, valueCols, outColNames etc.) for this ptfDesc.
       */

      ArrayList<ExprNodeDesc> partCols = new ArrayList<ExprNodeDesc>();
      ArrayList<ExprNodeDesc> valueCols = new ArrayList<ExprNodeDesc>();
      ArrayList<ExprNodeDesc> orderCols = new ArrayList<ExprNodeDesc>();
      Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();
      List<String> outputColumnNames = new ArrayList<String>();
      StringBuilder orderString = new StringBuilder();

      /*
       * Use the input RR of TableScanOperator in case there is no map-side
       * reshape of input.
       * If the parent of ReduceSinkOperator is PTFOperator, use it's
       * output RR.
       */
      buildPTFReduceSinkDetails(tabDef,
          rr,
          partCols,
          valueCols,
          orderCols,
          colExprMap,
          outputColumnNames,
          orderString,
          rsOpRR,
          extractOpRR);

      input = putOpInsertMap(OperatorFactory.getAndMakeChild(PlanUtils
          .getReduceSinkDesc(orderCols,
              valueCols, outputColumnNames, false,
              -1, partCols, orderString.toString(), -1, AcidUtils.Operation.NOT_ACID),
          new RowSchema(rsOpRR.getColumnInfos()), input), rsOpRR);
      input.setColumnExprMap(colExprMap);
    }

    /*
     * 3. build Reduce-side Op Graph
     */
    {
      /*
       * b. Construct Extract Operator.
       */
      input = putOpInsertMap(OperatorFactory.getAndMakeChild(
          new ExtractDesc(
              new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo,
                  Utilities.ReduceField.VALUE
                  .toString(), "", false)),
          new RowSchema(extractOpRR.getColumnInfos()),
          input), extractOpRR);

      /*
       * c. Rebuilt the QueryDef.
       * Why?
       * - so that the ExprNodeDescriptors in the QueryDef are based on the
       *   Extract Operator's RowResolver
       */
      rr = opParseCtx.get(input).getRowResolver();
      ptfDesc = translatePTFInvocationSpec(ptfQSpec, rr);

      /*
       * d. Construct PTF Operator.
       */
      RowResolver ptfOpRR = ptfDesc.getFuncDef().getOutputShape().getRr();
      input = putOpInsertMap(OperatorFactory.getAndMakeChild(ptfDesc,
          new RowSchema(ptfOpRR.getColumnInfos()),
          input), ptfOpRR);

    }

    return input;

  }

//--------------------------- Windowing handling: PTFInvocationSpec to PTFDesc --------------------

  Operator genWindowingPlan(WindowingSpec wSpec, Operator input) throws SemanticException {
    wSpec.validateAndMakeEffective();
    WindowingComponentizer groups = new WindowingComponentizer(wSpec);
    RowResolver rr = opParseCtx.get(input).getRowResolver();

    while(groups.hasNext() ) {
      wSpec = groups.next(conf, this, unparseTranslator, rr);
      input = genReduceSinkPlanForWindowing(wSpec, rr, input);
      rr = opParseCtx.get(input).getRowResolver();
      PTFTranslator translator = new PTFTranslator();
      PTFDesc ptfDesc = translator.translate(wSpec, this, conf, rr, unparseTranslator);
      RowResolver ptfOpRR = ptfDesc.getFuncDef().getOutputShape().getRr();
      input = putOpInsertMap(OperatorFactory.getAndMakeChild(ptfDesc,
          new RowSchema(ptfOpRR.getColumnInfos()),
          input), ptfOpRR);
      input = genSelectAllDesc(input);
      rr = ptfOpRR;
    }

    return input;
  }

  private Operator genReduceSinkPlanForWindowing(WindowingSpec spec,
      RowResolver inputRR, Operator input) throws SemanticException{
    
    ArrayList<ExprNodeDesc> partCols = new ArrayList<ExprNodeDesc>();
    ArrayList<ExprNodeDesc> orderCols = new ArrayList<ExprNodeDesc>();
    StringBuilder order = new StringBuilder();

    for (PartitionExpression partCol : spec.getQueryPartitionSpec().getExpressions()) {
      ExprNodeDesc partExpr = genExprNodeDesc(partCol.getExpression(), inputRR);
      partCols.add(partExpr);
      orderCols.add(partExpr);
      order.append('+');
    }

    if (spec.getQueryOrderSpec() != null) {
      for (OrderExpression orderCol : spec.getQueryOrderSpec().getExpressions()) {
        String orderString = orderCol.getOrder().name();
        if (orderString.equals("ASC")) {
          order.append('+');
        } else {
          order.append('-');
        }
        orderCols.add(genExprNodeDesc(orderCol.getExpression(), inputRR));
      }
    }

    return genReduceSinkPlan(input, partCols, orderCols, order.toString(), -1);
  }

  public static ArrayList<WindowExpressionSpec> parseSelect(String selectExprStr)
      throws SemanticException
  {
    ASTNode selNode = null;
    try {
      ParseDriver pd = new ParseDriver();
      selNode = pd.parseSelect(selectExprStr, null);
    } catch (ParseException pe) {
      throw new SemanticException(pe);
    }

    ArrayList<WindowExpressionSpec> selSpec = new ArrayList<WindowExpressionSpec>();
    int childCount = selNode.getChildCount();
    for (int i = 0; i < childCount; i++) {
      ASTNode selExpr = (ASTNode) selNode.getChild(i);
      if (selExpr.getType() != HiveParser.TOK_SELEXPR) {
        throw new SemanticException(String.format(
            "Only Select expressions supported in dynamic select list: %s", selectExprStr));
      }
      ASTNode expr = (ASTNode) selExpr.getChild(0);
      if (expr.getType() == HiveParser.TOK_ALLCOLREF) {
        throw new SemanticException(
            String.format("'%s' column not allowed in dynamic select list", selectExprStr));
      }
      ASTNode aliasNode = selExpr.getChildCount() > 1
          && selExpr.getChild(1).getType() == HiveParser.Identifier ?
          (ASTNode) selExpr.getChild(1) : null;
      String alias = null;
      if ( aliasNode != null ) {
        alias = aliasNode.getText();
      }
      else {
        String[] tabColAlias = getColAlias(selExpr, null, null, true, -1);
        alias = tabColAlias[1];
      }
      WindowExpressionSpec exprSpec = new WindowExpressionSpec();
      exprSpec.setAlias(alias);
      exprSpec.setExpression(expr);
      selSpec.add(exprSpec);
    }

    return selSpec;
  }

  private void addAlternateGByKeyMappings(ASTNode gByExpr, ColumnInfo colInfo,
      Operator<? extends OperatorDesc> reduceSinkOp, RowResolver gByRR) {
    if ( gByExpr.getType() == HiveParser.DOT
          && gByExpr.getChild(0).getType() == HiveParser.TOK_TABLE_OR_COL ) {
      String tab_alias = BaseSemanticAnalyzer.unescapeIdentifier(gByExpr
                .getChild(0).getChild(0).getText());
      String col_alias = BaseSemanticAnalyzer.unescapeIdentifier(
          gByExpr.getChild(1).getText());
      gByRR.put(tab_alias, col_alias, colInfo);
    } else if ( gByExpr.getType() == HiveParser.TOK_TABLE_OR_COL ) {
      String col_alias = BaseSemanticAnalyzer.unescapeIdentifier(gByExpr
              .getChild(0).getText());
      String tab_alias = null;
      /*
       * If the input to the GBy has a tab alias for the column, then add an entry
       * based on that tab_alias.
       * For e.g. this query:
       * select b.x, count(*) from t1 b group by x
       * needs (tab_alias=b, col_alias=x) in the GBy RR.
       * tab_alias=b comes from looking at the RowResolver that is the ancestor
       * before any GBy/ReduceSinks added for the GBY operation.
       */
      Operator<? extends OperatorDesc> parent = reduceSinkOp;
      while ( parent instanceof ReduceSinkOperator ||
          parent instanceof GroupByOperator ) {
        parent = parent.getParentOperators().get(0);
      }
      RowResolver parentRR = opParseCtx.get(parent).getRowResolver();
      try {
        ColumnInfo pColInfo = parentRR.get(tab_alias, col_alias);
        tab_alias = pColInfo == null ? null : pColInfo.getTabAlias();
      } catch(SemanticException se) {
      }
      gByRR.put(tab_alias, col_alias, colInfo);
    }
  }

  private WriteEntity.WriteType determineWriteType(LoadTableDesc ltd, boolean isNonNativeTable) {
    // Don't know the characteristics of non-native tables,
    // and don't have a rational way to guess, so assume the most
    // conservative case.
    if (isNonNativeTable) return WriteEntity.WriteType.INSERT_OVERWRITE;
    else return (ltd.getReplace() ? WriteEntity.WriteType.INSERT_OVERWRITE :
        WriteEntity.WriteType.INSERT);
  }

  // Even if the table is of Acid type, if we aren't working with an Acid compliant TxnManager
  // then return false.
  private boolean isAcidTable(Table tab) {
    if (tab == null) return false;
    if (!SessionState.get().getTxnMgr().supportsAcid()) return false;
    String tableIsTransactional =
        tab.getProperty(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL);
    return tableIsTransactional != null && tableIsTransactional.equalsIgnoreCase("true");
  }

  private boolean isAcidOutputFormat(Class<? extends OutputFormat> of) {
    Class<?>[] interfaces = of.getInterfaces();
    for (Class<?> iface : interfaces) {
      if (iface.equals(AcidOutputFormat.class)) {
        return true;
      }
    }
    return false;
  }

  // Note that this method assumes you have already decided this is an Acid table.  It cannot
  // figure out if a table is Acid or not.
  private AcidUtils.Operation getAcidType() {
    return deleting() ? AcidUtils.Operation.DELETE :
        (updating() ? AcidUtils.Operation.UPDATE :
            AcidUtils.Operation.INSERT);
  }

  private AcidUtils.Operation getAcidType(Class<? extends OutputFormat> of) {
    if (SessionState.get() == null || !SessionState.get().getTxnMgr().supportsAcid()) {
      return AcidUtils.Operation.NOT_ACID;
    } else if (isAcidOutputFormat(of)) {
      return getAcidType();
    } else {
      return AcidUtils.Operation.NOT_ACID;
    }
  }

  protected boolean updating() {
    return false;
  }

  protected boolean deleting() {
    return false;
  }
  public static ASTNode genSelectDIAST(RowResolver rr) {
    HashMap<String, LinkedHashMap<String, ColumnInfo>> map = rr.getRslvMap();
    ASTNode selectDI = new ASTNode(new CommonToken(HiveParser.TOK_SELECTDI, "TOK_SELECTDI"));
    for (String tabAlias : map.keySet()) {
      for (Entry<String, ColumnInfo> entry : map.get(tabAlias).entrySet()) {
        selectDI.addChild(buildSelExprSubTree(tabAlias, entry.getKey()));
      }
    }
    return selectDI;
  }
  private static ASTNode buildSelExprSubTree(String tableAlias, String col) {
    ASTNode selexpr = new ASTNode(new CommonToken(HiveParser.TOK_SELEXPR, "TOK_SELEXPR"));
    ASTNode tableOrCol = new ASTNode(new CommonToken(HiveParser.TOK_TABLE_OR_COL,
        "TOK_TABLE_OR_COL"));
    ASTNode dot = new ASTNode(new CommonToken(HiveParser.DOT, "."));
    tableOrCol.addChild(new ASTNode(new CommonToken(HiveParser.Identifier, tableAlias)));
    dot.addChild(tableOrCol);
    dot.addChild(new ASTNode(new CommonToken(HiveParser.Identifier, col)));
    selexpr.addChild(dot);
    return selexpr;
  }
}
