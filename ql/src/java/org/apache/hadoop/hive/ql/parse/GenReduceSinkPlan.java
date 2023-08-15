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

package org.apache.hadoop.hive.ql.parse;

import static java.util.Objects.nonNull;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.hadoop.hive.common.AcidConstants.SOFT_DELETE_TABLE;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.DYNAMICPARTITIONCONVERT;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVEARCHIVEENABLED;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_DEFAULT_STORAGE_HANDLER;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVESTATSDBCLASS;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_LOCATION;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_STORAGE;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.TABLE_IS_CTAS;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.DEFAULT_TABLE_TYPE;
import static org.apache.hadoop.hive.ql.ddl.view.create.AbstractCreateViewAnalyzer.validateTablesUsed;
import static org.apache.hadoop.hive.ql.optimizer.calcite.translator.ASTConverter.NON_FK_FILTERED;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.AccessControlException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

import org.antlr.runtime.ClassicToken;
import org.antlr.runtime.CommonToken;
import org.antlr.runtime.Token;
import org.antlr.runtime.TokenRewriteStream;
import org.antlr.runtime.tree.Tree;
import org.antlr.runtime.tree.TreeVisitor;
import org.antlr.runtime.tree.TreeVisitorAction;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.common.StatsSetupConst.StatDB;
import org.apache.hadoop.hive.common.StringInternUtils;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidTxnWriteIdList;
import org.apache.hadoop.hive.common.metrics.common.MetricsConstant;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.conf.HiveConf.ResultFileFormat;
import org.apache.hadoop.hive.conf.HiveConf.StrictChecks;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.TransactionalValidationListener;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.SQLCheckConstraint;
import org.apache.hadoop.hive.metastore.api.SQLDefaultConstraint;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint;
import org.apache.hadoop.hive.metastore.api.SourceTable;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryProperties;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.cache.results.CacheUsage;
import org.apache.hadoop.hive.ql.cache.results.QueryResultsCache;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.misc.hooks.InsertCommitHookDesc;
import org.apache.hadoop.hive.ql.ddl.table.constraint.ConstraintsUtils;
import org.apache.hadoop.hive.ql.ddl.table.convert.AlterTableConvertOperation;
import org.apache.hadoop.hive.ql.ddl.table.create.CreateTableDesc;
import org.apache.hadoop.hive.ql.ddl.table.create.like.CreateTableLikeDesc;
import org.apache.hadoop.hive.ql.ddl.table.misc.preinsert.PreInsertTableDesc;
import org.apache.hadoop.hive.ql.ddl.table.misc.properties.AlterTableUnsetPropertiesDesc;
import org.apache.hadoop.hive.ql.ddl.table.storage.skewed.SkewedTableUtils;
import org.apache.hadoop.hive.ql.ddl.view.create.CreateMaterializedViewDesc;
import org.apache.hadoop.hive.ql.ddl.view.materialized.update.MaterializedViewUpdateDesc;
import org.apache.hadoop.hive.ql.exec.AbstractMapJoinOperator;
import org.apache.hadoop.hive.ql.exec.ArchiveUtils;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluatorFactory;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.FunctionUtils;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.LimitOperator;
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
import org.apache.hadoop.hive.ql.exec.Utilities.ReduceField;
import org.apache.hadoop.hive.ql.exec.WindowFunctionInfo;
import org.apache.hadoop.hive.ql.exec.tez.TezTask;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity.WriteType;
import org.apache.hadoop.hive.ql.io.AcidInputFormat;
import org.apache.hadoop.hive.ql.io.AcidOutputFormat;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.AcidUtils.Operation;
import org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.io.NullRowsInputFormat;
import org.apache.hadoop.hive.ql.io.SchemaInferenceUtils;
import org.apache.hadoop.hive.ql.io.arrow.ArrowColumnarBatchSerDe;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.SemanticDispatcher;
import org.apache.hadoop.hive.ql.lib.SemanticGraphWalker;
import org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
import org.apache.hadoop.hive.ql.lockmgr.HiveTxnManager;
import org.apache.hadoop.hive.ql.lockmgr.LockException;
import org.apache.hadoop.hive.ql.metadata.DefaultConstraint;
import org.apache.hadoop.hive.ql.metadata.DummyPartition;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.metadata.InvalidTableException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.SessionHiveMetaStoreClient;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.optimizer.Optimizer;
import org.apache.hadoop.hive.ql.optimizer.QueryPlanPostProcessor;
import org.apache.hadoop.hive.ql.optimizer.Transform;
import org.apache.hadoop.hive.ql.optimizer.calcite.CalciteSemanticException;
import org.apache.hadoop.hive.ql.optimizer.calcite.CalciteSemanticException.UnsupportedFeature;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.ASTBuilder;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.HiveOpConverterPostProc;
import org.apache.hadoop.hive.ql.optimizer.lineage.Generator;
import org.apache.hadoop.hive.ql.optimizer.unionproc.UnionProcContext;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer.TableSpec.SpecType;
import org.apache.hadoop.hive.ql.parse.ExplainConfiguration.AnalyzeState;
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
import org.apache.hadoop.hive.ql.parse.WindowingSpec.Direction;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.WindowExpressionSpec;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.WindowFrameSpec;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.WindowFunctionSpec;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.WindowSpec;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.WindowType;
import org.apache.hadoop.hive.ql.parse.type.ExprNodeTypeCheck;
import org.apache.hadoop.hive.ql.parse.type.TypeCheckCtx;
import org.apache.hadoop.hive.ql.parse.type.TypeCheckProcFactory;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.DynamicPartitionCtx;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnListDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
import org.apache.hadoop.hive.ql.plan.ExprNodeFieldDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.FilterDesc;
import org.apache.hadoop.hive.ql.plan.FilterDesc.SampleDesc;
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
import org.apache.hadoop.hive.ql.plan.LoadTableDesc.LoadFileType;
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
import org.apache.hadoop.hive.ql.plan.mapper.AuxOpTreeSignature;
import org.apache.hadoop.hive.ql.plan.ptf.OrderExpressionDef;
import org.apache.hadoop.hive.ql.plan.ptf.PTFExpressionDef;
import org.apache.hadoop.hive.ql.plan.ptf.PartitionedTableFunctionDef;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.ResourceType;
import org.apache.hadoop.hive.ql.session.SessionStateUtil;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.Mode;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFArray;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFCardinalityViolation;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFHash;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFMurmurHash;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPOr;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFSurrogateKey;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTFInline;
import org.apache.hadoop.hive.ql.util.DirectionUtils;
import org.apache.hadoop.hive.ql.util.NullOrdering;
import org.apache.hadoop.hive.ql.util.ResourceDownloader;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.DelimitedJSONSerDe;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.MetadataTypedColumnsetSerDe;
import org.apache.hadoop.hive.serde2.NoOpFetchFormatter;
import org.apache.hadoop.hive.serde2.NullStructSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe2;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.thrift.ThriftJDBCBinarySerDe;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.shims.HadoopShims;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.math.IntMath;
import com.google.common.math.LongMath;

/**
 * XXX:
 */
public class GenReduceSinkPlan {
  private final Operator<? extends OperatorDesc> reduceOperator;
  private final RowResolver inputRR;
  private final Map<Operator<? extends OperatorDesc>, OpParseContext> finalOperatorMap;

  public GenReduceSinkPlan(String dest, QB qb, Operator<?> input,
                                     int numReducers, boolean hasOrderBy,
                                     HiveConf conf,
                                     HiveTxnManager txnMgr,
                                     ReadOnlySemanticAnalyzer sa,
                                     Map<Operator<? extends OperatorDesc>, OpParseContext> operatorMap,
                                     UnparseTranslator unparseTranslator
                                     ) throws SemanticException {
    inputRR = operatorMap.get(input).getRowResolver();

    // First generate the expression for the partition and sort keys
    // The cluster by clause / distribute by clause has the aliases for
    // partition function
    ASTNode partitionExprs = qb.getParseInfo().getClusterByForClause(dest);
    if (partitionExprs == null) {
      partitionExprs = qb.getParseInfo().getDistributeByForClause(dest);
    }
    List<ExprNodeDesc> partCols = new ArrayList<ExprNodeDesc>();
    if (partitionExprs != null) {
      int ccount = partitionExprs.getChildCount();
      for (int i = 0; i < ccount; ++i) {
        ASTNode cl = (ASTNode) partitionExprs.getChild(i);
        partCols.add(SemanticAnalyzer.genExprNodeDesc(cl, inputRR, true, false, unparseTranslator, conf));
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
        if (qb.getParseInfo().getDestLimit(dest) == null) {
          String error = StrictChecks.checkNoLimit(conf);
          if (error != null) {
            throw new SemanticException(SemanticAnalyzer.generateErrorMessage(sortExprs, error));
          }
        }
      }
    }
    List<ExprNodeDesc> sortCols = new ArrayList<ExprNodeDesc>();
    StringBuilder order = new StringBuilder();
    StringBuilder nullOrder = new StringBuilder();
    if (sortExprs != null) {
      int ccount = sortExprs.getChildCount();
      for (int i = 0; i < ccount; ++i) {
        ASTNode cl = (ASTNode) sortExprs.getChild(i);

        if (cl.getType() == HiveParser.TOK_TABSORTCOLNAMEASC) {
          // SortBy ASC
          order.append("+");
          cl = (ASTNode) cl.getChild(0);
          if (cl.getType() == HiveParser.TOK_NULLS_FIRST) {
            nullOrder.append("a");
          } else if (cl.getType() == HiveParser.TOK_NULLS_LAST) {
            nullOrder.append("z");
          } else {
            throw new SemanticException(
                "Unexpected null ordering option: " + cl.getType());
          }
          cl = (ASTNode) cl.getChild(0);
        } else if (cl.getType() == HiveParser.TOK_TABSORTCOLNAMEDESC) {
          // SortBy DESC
          order.append("-");
          cl = (ASTNode) cl.getChild(0);
          if (cl.getType() == HiveParser.TOK_NULLS_FIRST) {
            nullOrder.append("a");
          } else if (cl.getType() == HiveParser.TOK_NULLS_LAST) {
            nullOrder.append("z");
          } else {
            throw new SemanticException(
                "Unexpected null ordering option: " + cl.getType());
          }
          cl = (ASTNode) cl.getChild(0);
        } else {
          // ClusterBy
          order.append("+");
          nullOrder.append("a");
        }
        ExprNodeDesc exprNode = SemanticAnalyzer.genExprNodeDesc(cl, inputRR, true, false, unparseTranslator, conf);
        sortCols.add(exprNode);
      }
    }

    Table dest_tab = qb.getMetaData().getDestTableForAlias(dest);
    AcidUtils.Operation acidOp = Operation.NOT_ACID;
    if (AcidUtils.isTransactionalTable(dest_tab)) {
      acidOp = SemanticAnalyzer.getAcidType(Utilities.getTableDesc(dest_tab).getOutputFileFormatClass(), dest,
          AcidUtils.isInsertOnlyTable(dest_tab), txnMgr);
    }
    boolean isCompaction = false;
    if (dest_tab != null && dest_tab.getParameters() != null) {
      isCompaction = AcidUtils.isCompactionTable(dest_tab.getParameters());
    }
    GenReduceSinkPlan finalGenReduceSinkPlan = new GenReduceSinkPlan(
        input, partCols, sortCols, order.toString(), nullOrder.toString(),
        numReducers, acidOp, true, isCompaction, sa, operatorMap);
    reduceOperator = finalGenReduceSinkPlan.getOperator();
    finalOperatorMap = finalGenReduceSinkPlan.getOperatorMap();
    if (reduceOperator.getParentOperators().size() == 1 &&
        reduceOperator.getParentOperators().get(0) instanceof ReduceSinkOperator) {
      ((ReduceSinkOperator) reduceOperator.getParentOperators().get(0))
          .getConf().setHasOrderBy(hasOrderBy);
    }
  }

  public GenReduceSinkPlan(Operator<?> input,
                                     List<ExprNodeDesc> partitionCols, List<ExprNodeDesc> sortCols,
                                     String sortOrder, String nullOrder, int numReducers, AcidUtils.Operation acidOp, boolean isCompaction,
                                     ReadOnlySemanticAnalyzer sa,
                                     Map<Operator<? extends OperatorDesc>, OpParseContext> operatorMap
                                     )
      throws SemanticException {
    this(input, partitionCols, sortCols, sortOrder, nullOrder, numReducers, acidOp, false, isCompaction, sa, operatorMap);
  }

  @SuppressWarnings("nls")
  public GenReduceSinkPlan(Operator<?> input, List<ExprNodeDesc> partitionCols, List<ExprNodeDesc> sortCols,
                                     String sortOrder, String nullOrder, int numReducers, AcidUtils.Operation acidOp,
                                     boolean pullConstants, boolean isCompaction,
      ReadOnlySemanticAnalyzer sa,
      Map<Operator<? extends OperatorDesc>, OpParseContext> operatorMap
                                     ) throws SemanticException {
    finalOperatorMap = new HashMap<>();
    inputRR = operatorMap.get(input).getRowResolver();

    Operator dummy = Operator.createDummy();
    dummy.setParentOperators(Arrays.asList(input));

    List<ExprNodeDesc> newSortCols = new ArrayList<ExprNodeDesc>();
    StringBuilder newSortOrder = new StringBuilder();
    StringBuilder newNullOrder = new StringBuilder();
    List<ExprNodeDesc> sortColsBack = new ArrayList<ExprNodeDesc>();
    for (int i = 0; i < sortCols.size(); i++) {
      ExprNodeDesc sortCol = sortCols.get(i);
      // If we are not pulling constants, OR
      // we are pulling constants but this is not a constant
      if (!pullConstants || !(sortCol instanceof ExprNodeConstantDesc)) {
        newSortCols.add(sortCol);
        newSortOrder.append(sortOrder.charAt(i));
        newNullOrder.append(nullOrder.charAt(i));
        sortColsBack.add(ExprNodeDescUtils.backtrack(sortCol, dummy, input));
      }
    }

    // For the generation of the values expression just get the inputs
    // signature and generate field expressions for those
    RowResolver rsRR = new RowResolver();
    List<String> outputColumns = new ArrayList<String>();
    List<ExprNodeDesc> valueCols = new ArrayList<ExprNodeDesc>();
    List<ExprNodeDesc> valueColsBack = new ArrayList<ExprNodeDesc>();
    Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();
    List<ExprNodeDesc> constantCols = new ArrayList<ExprNodeDesc>();

    List<ColumnInfo> columnInfos = inputRR.getColumnInfos();

    int[] index = new int[columnInfos.size()];
    for (int i = 0; i < index.length; i++) {
      ColumnInfo colInfo = columnInfos.get(i);
      String[] nm = inputRR.reverseLookup(colInfo.getInternalName());
      String[] nm2 = inputRR.getAlternateMappings(colInfo.getInternalName());
      ExprNodeColumnDesc value = new ExprNodeColumnDesc(colInfo);

      // backtrack can be null when input is script operator
      ExprNodeDesc valueBack = ExprNodeDescUtils.backtrack(value, dummy, input);
      if (pullConstants && valueBack instanceof ExprNodeConstantDesc) {
        // ignore, it will be generated by SEL op
        index[i] = Integer.MAX_VALUE;
        constantCols.add(valueBack);
        continue;
      }
      int kindex = valueBack == null ? -1 : ExprNodeDescUtils.indexOf(valueBack, sortColsBack);
      if (kindex >= 0) {
        index[i] = kindex;
        ColumnInfo newColInfo = new ColumnInfo(colInfo);
        newColInfo.setInternalName(Utilities.ReduceField.KEY + ".reducesinkkey" + kindex);
        newColInfo.setTabAlias(nm[0]);
        rsRR.put(nm[0], nm[1], newColInfo);
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
      String outputColName = SemanticAnalyzer.getColumnInternalName(valueCols.size());

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

    ReduceSinkDesc rsdesc = PlanUtils.getReduceSinkDesc(newSortCols, valueCols, outputColumns,
        false, -1, partitionCols, newSortOrder.toString(), newNullOrder.toString(), sa.getDefaultNullOrdering(),
        numReducers, acidOp, isCompaction);
    Operator interim = GenFileSinkPlan.createOperator(rsdesc,
        new RowSchema(rsRR.getColumnInfos()), input);

    finalOperatorMap.put(interim, new OpParseContext(rsRR));

    List<String> keyColNames = rsdesc.getOutputKeyColumnNames();
    for (int i = 0 ; i < keyColNames.size(); i++) {
      colExprMap.put(Utilities.ReduceField.KEY + "." + keyColNames.get(i), newSortCols.get(i));
    }
    List<String> valueColNames = rsdesc.getOutputValueColumnNames();
    for (int i = 0 ; i < valueColNames.size(); i++) {
      colExprMap.put(Utilities.ReduceField.VALUE + "." + valueColNames.get(i), valueCols.get(i));
    }
    interim.setColumnExprMap(colExprMap);

    RowResolver selectRR = new RowResolver();
    List<ExprNodeDesc> selCols = new ArrayList<ExprNodeDesc>();
    List<String> selOutputCols = new ArrayList<String>();
    Map<String, ExprNodeDesc> selColExprMap = new HashMap<String, ExprNodeDesc>();

    Iterator<ExprNodeDesc> constants = constantCols.iterator();
    for (int i = 0; i < index.length; i++) {
      ColumnInfo prev = columnInfos.get(i);
      String[] nm = inputRR.reverseLookup(prev.getInternalName());
      String[] nm2 = inputRR.getAlternateMappings(prev.getInternalName());
      ColumnInfo info = new ColumnInfo(prev);

      ExprNodeDesc desc;
      if (index[i] == Integer.MAX_VALUE) {
        desc = constants.next();
      } else {
        String field;
        if (index[i] >= 0) {
          field = Utilities.ReduceField.KEY + "." + keyColNames.get(index[i]);
        } else {
          field = Utilities.ReduceField.VALUE + "." + valueColNames.get(-index[i] - 1);
        }
        desc = new ExprNodeColumnDesc(info.getType(),
            field, info.getTabAlias(), info.getIsVirtualCol());
      }
      selCols.add(desc);

      String internalName = SemanticAnalyzer.getColumnInternalName(i);
      info.setInternalName(internalName);
      selectRR.put(nm[0], nm[1], info);
      if (nm2 != null) {
        selectRR.addMappingOnly(nm2[0], nm2[1], info);
      }
      selOutputCols.add(internalName);
      selColExprMap.put(internalName, desc);
    }
    SelectDesc select = new SelectDesc(selCols, selOutputCols);
    reduceOperator = GenFileSinkPlan.createOperator(select,
        new RowSchema(selectRR.getColumnInfos()), interim);
    finalOperatorMap.put(reduceOperator, new OpParseContext(selectRR));
    reduceOperator.setColumnExprMap(selColExprMap);
  }

  public Operator getOperator() {
    return reduceOperator;
  }

  public Map<Operator<? extends OperatorDesc>, OpParseContext> getOperatorMap() {
    return finalOperatorMap;
  }
}
