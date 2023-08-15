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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GenConversionSelectOperator {
  protected static final Logger LOG = LoggerFactory.getLogger(GenConversionSelectOperator.class.getName());
 
  private AtomicBoolean converted = new AtomicBoolean(false);
  private Operator<? extends OperatorDesc> conversionSelectOperator;
  private RowResolver rowResolver;

  public GenConversionSelectOperator(
      String dest, QB qb, Operator input,
      Deserializer deserializer, DynamicPartitionCtx dpCtx, List<FieldSchema> parts, Table table,
      Map<Operator<? extends OperatorDesc>, OpParseContext> opParseCtx,
      HiveConf conf
      ) throws SemanticException {
    conversionSelectOperator = input;
    StructObjectInspector oi = null;
    try {
      oi = (StructObjectInspector) deserializer.getObjectInspector();
    } catch (Exception e) {
      throw new SemanticException(e);
    }

    // Check column number
    List<? extends StructField> tableFields = oi.getAllStructFieldRefs();
    boolean dynPart = HiveConf.getBoolVar(conf, HiveConf.ConfVars.DYNAMICPARTITIONING);
    List<ColumnInfo> rowFields = opParseCtx.get(input).getRowResolver().getColumnInfos();
    int inColumnCnt = rowFields.size();
    int outColumnCnt = tableFields.size();

    // if target table is always unpartitioned, then the output object inspector will already contain the partition cols
    // too, therefore we shouldn't add the partition col num to the output col num
    boolean alreadyContainsPartCols = Optional.ofNullable(table)
        .map(Table::getStorageHandler)
        .map(HiveStorageHandler::alwaysUnpartitioned)
        .orElse(Boolean.FALSE);

    if (dynPart && dpCtx != null && !alreadyContainsPartCols) {
      outColumnCnt += dpCtx.getNumDPCols();
    }

    // The numbers of input columns and output columns should match for regular query
    // Impala partial width inserts are handled later by ImpalaPlanner
    if (!SemanticAnalyzer.updating(dest) && !SemanticAnalyzer.deleting(dest) && inColumnCnt != outColumnCnt) {
      String reason = "Table " + dest + " has " + outColumnCnt
          + " columns, but query has " + inColumnCnt + " columns.";
      throw new SemanticException(ASTErrorUtils.getMsg(
          ErrorMsg.TARGET_TABLE_COLUMN_MISMATCH.getMsg(),
          qb.getParseInfo().getDestForClause(dest), reason));
    }

    // Check column types
    int columnNumber = tableFields.size();
    List<ExprNodeDesc> expressions = new ArrayList<ExprNodeDesc>(columnNumber);

    // MetadataTypedColumnsetSerDe does not need type conversions because it
    // does the conversion to String by itself.
    if (!(deserializer instanceof MetadataTypedColumnsetSerDe) && !SemanticAnalyzer.deleting(dest)) {

      // If we're updating, add the required virtual columns.
      int virtualColumnSize = SemanticAnalyzer.updating(dest) ? AcidUtils.getAcidVirtualColumns(table).size() : 0;
      for (int i = 0; i < virtualColumnSize; i++) {
        expressions.add(new ExprNodeColumnDesc(rowFields.get(i).getType(),
            rowFields.get(i).getInternalName(), "", true));
      }

      // here only deals with non-partition columns. We deal with partition columns next
      int rowFieldsOffset = expressions.size();
      for (int i = 0; i < columnNumber; i++) {
        ExprNodeDesc column = handleConversion(tableFields.get(i), rowFields.get(rowFieldsOffset + i), converted, dest, i, qb);
        expressions.add(column);
      }

      // For Non-Native ACID tables we should convert the new values as well
      rowFieldsOffset = expressions.size();
      if (SemanticAnalyzer.updating(dest) && AcidUtils.isNonNativeAcidTable(table, true)) {
        for (int i = 0; i < columnNumber; i++) {
          ExprNodeDesc column = handleConversion(tableFields.get(i), rowFields.get(rowFieldsOffset + i), converted, dest, i, qb);
          expressions.add(column);
        }
      }

      // deal with dynamic partition columns
      rowFieldsOffset = expressions.size();
      if (dynPart && dpCtx != null && dpCtx.getNumDPCols() > 0) {
        // rowFields contains non-partitioned columns (tableFields) followed by DP columns
        for (int dpColIdx = 0; dpColIdx < rowFields.size() - rowFieldsOffset; ++dpColIdx) {

          // create ExprNodeDesc
          ColumnInfo inputColumn = rowFields.get(dpColIdx + rowFieldsOffset);
          TypeInfo inputTypeInfo = inputColumn.getType();
          ExprNodeDesc column =
              new ExprNodeColumnDesc(inputTypeInfo, inputColumn.getInternalName(), "", true);

          // Cast input column to destination column type if necessary.
          if (conf.getBoolVar(DYNAMICPARTITIONCONVERT)) {
            if (parts != null && !parts.isEmpty()) {
              String destPartitionName = dpCtx.getDPColNames().get(dpColIdx);
              FieldSchema destPartitionFieldSchema = parts.stream()
                  .filter(dynamicPartition -> dynamicPartition.getName().equals(destPartitionName))
                  .findFirst().orElse(null);
              if (destPartitionFieldSchema == null) {
                throw new IllegalStateException("Partition schema for dynamic partition " +
                    destPartitionName + " not found in DynamicPartitionCtx.");
              }
              String partitionType = destPartitionFieldSchema.getType();
              if (partitionType == null) {
                throw new IllegalStateException("Couldn't get FieldSchema for partition" +
                    destPartitionFieldSchema.getName());
              }
              PrimitiveTypeInfo partitionTypeInfo =
                  TypeInfoFactory.getPrimitiveTypeInfo(partitionType);
              if (!partitionTypeInfo.equals(inputTypeInfo)) {
                column = ExprNodeTypeCheck.getExprNodeDefaultExprProcessor()
                    .createConversionCast(column, partitionTypeInfo);
                converted.set(true);
              }
            } else {
              LOG.warn("Partition schema for dynamic partition " + inputColumn.getAlias() + " ("
                  + inputColumn.getInternalName() + ") not found in DynamicPartitionCtx. "
                  + "This is expected with a CTAS.");
            }
          }
          expressions.add(column);
        }
      }
    }

    if (converted.get()) {
      // add the select operator
      rowResolver = new RowResolver();
      List<String> colNames = new ArrayList<String>();
      Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();
      for (int i = 0; i < expressions.size(); i++) {
        String name = HiveConf.getColumnInternalName(i);
        rowResolver.put("", name, new ColumnInfo(name, expressions.get(i)
            .getTypeInfo(), "", false));
        colNames.add(name);
        colExprMap.put(name, expressions.get(i));
      }
      conversionSelectOperator = GenFileSinkPlan.createOperator(
          new SelectDesc(expressions, colNames), new RowSchema(rowResolver
              .getColumnInfos()), input);
      conversionSelectOperator.setColumnExprMap(colExprMap);
    }
  }

  public boolean createdOperator() {
    return converted.get();
  }

  public Operator<? extends OperatorDesc> getOperator() {
    return conversionSelectOperator;
  }

  public RowResolver getRowResolver() {
    return rowResolver;
  }

  /**
   * Creates an expression for converting from a table column to a row column. For example:
   * The table column is int but the query provides a string in the row, then we need to cast automatically.
   * @param tableField The target table column
   * @param rowField The source row column
   * @param conversion The value of this boolean is set to true if we detect that a conversion is needed. This is a
   *                   hidden return value hidden here, to notify the caller that a cast was needed.
   * @param dest The destination table for the error message
   * @param columnNum The destination column id for the error message
   * @return The Expression describing the selected column. Note that `conversion` can be considered as a return value
   *         as well
   * @throws SemanticException If conversion were needed, but automatic conversion is not available
   */
  private static ExprNodeDesc handleConversion(StructField tableField, ColumnInfo rowField, AtomicBoolean conversion, String dest,
      int columnNum, QB qb)
      throws SemanticException {
    ObjectInspector tableFieldOI = tableField
        .getFieldObjectInspector();
    TypeInfo tableFieldTypeInfo = TypeInfoUtils
        .getTypeInfoFromObjectInspector(tableFieldOI);
    TypeInfo rowFieldTypeInfo = rowField.getType();
    ExprNodeDesc column = new ExprNodeColumnDesc(rowFieldTypeInfo,
        rowField.getInternalName(), "", false,
        rowField.isSkewedCol());
    // LazySimpleSerDe can convert any types to String type using
    // JSON-format. However, we may add more operators.
    // Thus, we still keep the conversion.
    if (!tableFieldTypeInfo.equals(rowFieldTypeInfo)) {
      // need to do some conversions here
      conversion.set(true);
      if (tableFieldTypeInfo.getCategory() != Category.PRIMITIVE) {
        // cannot convert to complex types
        column = null;
      } else {
        column = ExprNodeTypeCheck.getExprNodeDefaultExprProcessor()
            .createConversionCast(column, (PrimitiveTypeInfo)tableFieldTypeInfo);
      }
      if (column == null) {
        String reason = "Cannot convert column " + columnNum + " from "
            + rowFieldTypeInfo + " to " + tableFieldTypeInfo + ".";
        throw new SemanticException(ASTErrorUtils.getMsg(
            ErrorMsg.TARGET_TABLE_COLUMN_MISMATCH.getMsg(),
            qb.getParseInfo().getDestForClause(dest), reason));
      }
    }
    return column;
  }
}
