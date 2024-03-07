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

package org.apache.hadoop.hive.ql.optimizer.ppr;

import java.util.AbstractSequentialList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.StrictChecks;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.PrunerUtils;
import org.apache.hadoop.hive.ql.optimizer.Transform;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.PrunedPartitionList;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeFieldDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPOr;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;

/**
 * The transformation step that does partition pruning.
 *
 */
public class PartitionPruner extends Transform {

  // The log
  public static final String CLASS_NAME = PartitionPruner.class.getName();
  public static final Logger LOG = LoggerFactory.getLogger(CLASS_NAME);

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.hadoop.hive.ql.optimizer.Transform#transform(org.apache.hadoop
   * .hive.ql.parse.ParseContext)
   */
  @Override
  public ParseContext transform(ParseContext pctx) throws SemanticException {

    // create a the context for walking operators
    OpWalkerCtx opWalkerCtx = new OpWalkerCtx(pctx.getOpToPartPruner());

    /* Move logic to PrunerUtils.walkOperatorTree() so that it can be reused. */
    PrunerUtils.walkOperatorTree(pctx, opWalkerCtx, OpProcFactory.getFilterProc(),
        OpProcFactory.getDefaultProc());
    return pctx;
  }

  /**
   * Find out whether the condition only contains partitioned columns. Note that
   * if the table is not partitioned, the function always returns true.
   * condition.
   *
   * @param tab
   *          the table object
   * @param expr
   *          the pruner expression for the table
   */
  public static boolean onlyContainsPartnCols(Table tab, ExprNodeDesc expr) {
    if (!tab.isPartitioned() || (expr == null)) {
      return true;
    }

    if (expr instanceof ExprNodeColumnDesc) {
      String colName = ((ExprNodeColumnDesc) expr).getColumn();
      return tab.isPartitionKey(colName);
    }

    // It cannot contain a non-deterministic function
    if ((expr instanceof ExprNodeGenericFuncDesc)
        && !FunctionRegistry.isConsistentWithinQuery(((ExprNodeGenericFuncDesc) expr)
        .getGenericUDF())) {
      return false;
    }

    // All columns of the expression must be partitioned columns
    List<ExprNodeDesc> children = expr.getChildren();
    if (children != null) {
      for (int i = 0; i < children.size(); i++) {
        if (!onlyContainsPartnCols(tab, children.get(i))) {
          return false;
        }
      }
    }

    return true;
  }

  /**
   * Get the partition list for the TS operator that satisfies the partition pruner
   * condition.
   */
  public static PrunedPartitionList prune(TableScanOperator ts, ParseContext parseCtx,
      String alias) throws SemanticException {
    return prune(ts.getConf().getTableMetadata(), parseCtx.getOpToPartPruner().get(ts),
        parseCtx.getConf(), alias, parseCtx.getPrunedPartitions());
  }

  /**
   * Get the partition list for the table that satisfies the partition pruner
   * condition.
   *
   * @param tab
   *          the table object for the alias
   * @param prunerExpr
   *          the pruner expression for the alias
   * @param conf
   *          for checking whether "strict" mode is on.
   * @param alias
   *          for generating error message only.
   * @param prunedPartitionsMap
   *          cached result for the table
   * @return the partition list for the table that satisfies the partition
   *         pruner condition.
   * @throws SemanticException
   */
  public static PrunedPartitionList prune(Table tab, ExprNodeDesc prunerExpr,
      HiveConf conf, String alias, Map<String, PrunedPartitionList> prunedPartitionsMap)
          throws SemanticException {

    if (LOG.isTraceEnabled()) {
      LOG.trace("Started pruning partition");
      LOG.trace("dbname = " + tab.getDbName());
      LOG.trace("tabname = " + tab.getTableName());
      LOG.trace("prune Expression = " + (prunerExpr == null ? "" : prunerExpr));
    }

    String key = tab.getFullyQualifiedName() + ";";
    if (tab.getMetaTable() != null) {
      key = tab.getFullyQualifiedName() + "." + tab.getMetaTable() + ";";
    } else if (tab.getSnapshotRef() != null) {
      key = tab.getFullyQualifiedName() + "." + tab.getSnapshotRef() + ";";
    }

    if (!tab.isPartitioned()) {
      // If the table is not partitioned, return empty list.
      return getAllPartsFromCacheOrServer(tab, key, false, prunedPartitionsMap);
    }

    if (!hasColumnExpr(prunerExpr)) {
      // If the "strict" mode is on, we have to provide partition pruner for each table.
      String error = StrictChecks.checkNoPartitionFilter(conf);
      if (error != null) {
        throw new SemanticException(error + " No partition predicate for Alias \""
            + alias + "\" Table \"" + tab.getTableName() + "\"");
      }
    }

    if (prunerExpr == null) {
      // In non-strict mode and there is no predicates at all - get everything.
      return getAllPartsFromCacheOrServer(tab, key, false, prunedPartitionsMap);
    }

    Set<String> partColsUsedInFilter = new LinkedHashSet<String>();
    // Replace virtual columns with nulls. See javadoc for details.
    prunerExpr = removeNonPartCols(prunerExpr, extractPartColNames(tab), partColsUsedInFilter);
    // Remove all parts that are not partition columns. See javadoc for details.
    ExprNodeDesc compactExpr = compactExpr(prunerExpr.clone());
    String oldFilter = prunerExpr.getExprString(true);
    if (compactExpr == null || isBooleanExpr(compactExpr)) {
      if (isFalseExpr(compactExpr)) {
        return new PrunedPartitionList(tab, key + compactExpr.getExprString(true),
            Collections.emptySet(), Collections.emptyList(), false);
      }
      // For null and true values, return every partition
      return getAllPartsFromCacheOrServer(tab, key, true, prunedPartitionsMap);
    }

    String compactExprString = compactExpr.getExprString(true);
    LOG.debug("Filter w/ compacting: {}; filter w/o compacting: {}", compactExprString, oldFilter);
    key = key + compactExprString;
    PrunedPartitionList ppList = prunedPartitionsMap.get(key);
    if (ppList != null) {
      return ppList;
    }

    ppList = getPartitionsFromServer(tab, key, compactExpr,
        conf, alias, partColsUsedInFilter, oldFilter.equals(compactExprString));
    prunedPartitionsMap.put(key, ppList);
    return ppList;
  }

  private static PrunedPartitionList getAllPartsFromCacheOrServer(Table tab, String key, boolean unknownPartitions,
    Map<String, PrunedPartitionList> partsCache)  throws SemanticException {
    PrunedPartitionList ppList = partsCache == null ? null : partsCache.get(key);
    if (ppList != null) {
      return ppList;
    }
    Set<Partition> parts;
    try {
      parts = getAllPartitions(tab);
    } catch (HiveException e) {
      throw new SemanticException(e);
    }
    ppList = new PrunedPartitionList(tab, key, parts, Collections.emptyList(), unknownPartitions);
    if (partsCache != null) {
      partsCache.put(key, ppList);
    }
    return ppList;
  }

  static private boolean isBooleanExpr(ExprNodeDesc expr) {
    return  expr != null && expr instanceof ExprNodeConstantDesc &&
              ((ExprNodeConstantDesc)expr).getTypeInfo() instanceof PrimitiveTypeInfo &&
              ((PrimitiveTypeInfo)(((ExprNodeConstantDesc)expr).getTypeInfo())).
              getTypeName().equals(serdeConstants.BOOLEAN_TYPE_NAME);
  }
  static private boolean isTrueExpr(ExprNodeDesc expr) {
      return  isBooleanExpr(expr) &&
          ((ExprNodeConstantDesc)expr).getValue() != null &&
          ((ExprNodeConstantDesc)expr).getValue().equals(Boolean.TRUE);
  }
  static private boolean isFalseExpr(ExprNodeDesc expr) {
      return  isBooleanExpr(expr) &&
              ((ExprNodeConstantDesc)expr).getValue() != null &&
              ((ExprNodeConstantDesc)expr).getValue().equals(Boolean.FALSE);
  }

  /**
   * Taking a partition pruning expression, remove the null operands and non-partition columns.
   * The reason why there are null operands is ExprProcFactory classes, for example
   * PPRColumnExprProcessor.
   * @param expr original partition pruning expression.
   * @return partition pruning expression that only contains partition columns.
   */
  @VisibleForTesting
  static ExprNodeDesc compactExpr(ExprNodeDesc expr) {
    // If this is a constant boolean expression, return the value.
    if (expr == null) {
      return null;
    }
    if (expr instanceof ExprNodeConstantDesc) {
      if (((ExprNodeConstantDesc)expr).getValue() == null) {
        return null;
      }
      if (!isBooleanExpr(expr)) {
        throw new IllegalStateException("Unexpected non-boolean ExprNodeConstantDesc: "
            + expr.getExprString(true));
      }
      return expr;
    } else if (expr instanceof ExprNodeColumnDesc) {
      return expr;
    } else if (expr instanceof ExprNodeGenericFuncDesc) {
      GenericUDF udf = ((ExprNodeGenericFuncDesc)expr).getGenericUDF();
      boolean isAnd = udf instanceof GenericUDFOPAnd;
      boolean isOr = udf instanceof GenericUDFOPOr;
      List<ExprNodeDesc> children = expr.getChildren();

      if (isAnd) {
        // Non-partition expressions are converted to nulls.
        List<ExprNodeDesc> newChildren = new ArrayList<ExprNodeDesc>();
        boolean allTrue = true;
        for (ExprNodeDesc child : children) {
          ExprNodeDesc compactChild = compactExpr(child);
          if (compactChild != null) {
            if (!isTrueExpr(compactChild)) {
              newChildren.add(compactChild);
              allTrue = false;
            }
            if (isFalseExpr(compactChild)) {
              return new ExprNodeConstantDesc(Boolean.FALSE);
            }
          } else {
            allTrue = false;
          }
        }

        if (allTrue) {
          return new ExprNodeConstantDesc(Boolean.TRUE);
        }
        if (newChildren.size() == 0) {
          return null;
        }
        if (newChildren.size() == 1) {
          return newChildren.get(0);
        }

        // Nothing to compact, update expr with compacted children.
        ((ExprNodeGenericFuncDesc) expr).setChildren(newChildren);
      } else if (isOr) {
        // Non-partition expressions are converted to nulls.
        List<ExprNodeDesc> newChildren = new ArrayList<ExprNodeDesc>();
        boolean allFalse = true;
        boolean isNull = false;
        for (ExprNodeDesc child : children) {
          ExprNodeDesc compactChild = compactExpr(child);
          if (compactChild != null) {
            if (isTrueExpr(compactChild)) {
              return new ExprNodeConstantDesc(Boolean.TRUE);
            }
            if (!isNull && !isFalseExpr(compactChild)) {
              newChildren.add(compactChild);
              allFalse = false;
            }
          } else {
            isNull = true;
          }
        }

        if (isNull) {
          return null;
        }
        if (allFalse) {
          return new ExprNodeConstantDesc(Boolean.FALSE);
        }
        if (newChildren.size() == 1) {
          return newChildren.get(0);
        }

        // Nothing to compact, update expr with compacted children.
        ((ExprNodeGenericFuncDesc) expr).setChildren(newChildren);
      }

      return expr;
    } else {
      throw new IllegalStateException("Unexpected type of ExprNodeDesc: " + expr.getExprString(true));
    }
  }

  /**
   * See compactExpr. Some things in the expr are replaced with nulls for pruner, however
   * the virtual columns are not removed (ExprNodeColumnDesc cannot tell them apart from
   * partition columns), so we do it here.
   * The expression is only used to prune by partition name, so we have no business with VCs.
   * @param expr original partition pruning expression.
   * @param partCols list of partition columns for the table.
   * @param referred partition columns referred by expr
   * @return partition pruning expression that only contains partition columns from the list.
   */
  static private ExprNodeDesc removeNonPartCols(ExprNodeDesc expr, List<String> partCols,
      Set<String> referred) {
    if (expr instanceof ExprNodeFieldDesc) {
      // Column is not a partition column for the table,
      // as we do not allow partitions based on complex
      // list or struct fields.
      return new ExprNodeConstantDesc(expr.getTypeInfo(), null);
    }
    else if (expr instanceof ExprNodeColumnDesc) {
      String column = ((ExprNodeColumnDesc) expr).getColumn();
      if (!partCols.contains(column)) {
        // Column doesn't appear to be a partition column for the table.
        return new ExprNodeConstantDesc(expr.getTypeInfo(), null);
      }
      referred.add(column);
    }
    else if (expr instanceof ExprNodeGenericFuncDesc) {
      List<ExprNodeDesc> children = expr.getChildren();
      for (int i = 0; i < children.size(); ++i) {
        ExprNodeDesc other = removeNonPartCols(children.get(i), partCols, referred);
        if (ExprNodeDescUtils.isNullConstant(other)) {
          if (FunctionRegistry.isOpAnd(expr)) {
            // partcol=... AND nonpartcol=...   is replaced with partcol=... AND TRUE
            // which will be folded to partcol=...
            // This cannot be done also for OR
            Preconditions.checkArgument(expr.getTypeInfo().accept(TypeInfoFactory.booleanTypeInfo));
            other = new ExprNodeConstantDesc(expr.getTypeInfo(), true);
          } else {
            // Functions like NVL, COALESCE, CASE can change a
            // NULL introduced by a nonpart column removal into a non-null
            // and cause overaggressive prunning, missing data (incorrect result)
            return new ExprNodeConstantDesc(expr.getTypeInfo(), null);
          }
        }
        children.set(i, other);
      }
    }
    return expr;
  }

  /**
   * @param expr Expression.
   * @return True iff expr contains any non-native user-defined functions.
   */
  static private boolean hasUserFunctions(ExprNodeDesc expr) {
    if (!(expr instanceof ExprNodeGenericFuncDesc)) {
      return false;
    }
    if (!FunctionRegistry.isBuiltInFuncExpr((ExprNodeGenericFuncDesc) expr)) {
      return true;
    }
    for (ExprNodeDesc child : expr.getChildren()) {
      if (hasUserFunctions(child)) {
        return true;
      }
    }
    return false;
  }

  private static PrunedPartitionList getPartitionsFromServer(Table tab, final String key, final ExprNodeDesc compactExpr,
      HiveConf conf, String alias, Set<String> partColsUsedInFilter, boolean isPruningByExactFilter) throws SemanticException {
    try {

      // Finally, check the filter for non-built-in UDFs. If these are present, we cannot
      // do filtering on the server, and have to fall back to client path.
      boolean doEvalClientSide = hasUserFunctions(compactExpr);

      // Now filter.
      List<Partition> partitions = new ArrayList<Partition>();
      boolean hasUnknownPartitions = false;
      PerfLogger perfLogger = SessionState.getPerfLogger();
      if (!doEvalClientSide) {
        perfLogger.perfLogBegin(CLASS_NAME, PerfLogger.PARTITION_RETRIEVING);
        try {
          hasUnknownPartitions = Hive.get().getPartitionsByExpr(
              tab, compactExpr, conf, partitions);
        } catch (IMetaStoreClient.IncompatibleMetastoreException ime) {
          // TODO: backward compat for Hive <= 0.12. Can be removed later.
          LOG.warn("Metastore doesn't support getPartitionsByExpr", ime);
          doEvalClientSide = true;
        } finally {
          perfLogger.perfLogEnd(CLASS_NAME, PerfLogger.PARTITION_RETRIEVING);
        }
      }
      if (doEvalClientSide) {
        // Either we have user functions, or metastore is old version - filter names locally.
        hasUnknownPartitions = pruneBySequentialScan(tab, partitions, compactExpr, conf);
      }
      // The partitions are "unknown" if the call says so due to the expression
      // evaluator returning null for a partition, or if we sent a partial expression to
      // metastore and so some partitions may have no data based on other filters.
      return new PrunedPartitionList(tab, key,
          new LinkedHashSet<Partition>(partitions),
          new ArrayList<String>(partColsUsedInFilter),
          hasUnknownPartitions || !isPruningByExactFilter);
    } catch (SemanticException e) {
      throw e;
    } catch (Exception e) {
      throw new SemanticException(e);
    }
  }

  private static Set<Partition> getAllPartitions(Table tab) throws HiveException {
    PerfLogger perfLogger = SessionState.getPerfLogger();
    perfLogger.perfLogBegin(CLASS_NAME, PerfLogger.PARTITION_RETRIEVING);
    Set<Partition> result = Hive.get().getAllPartitionsOf(tab);
    perfLogger.perfLogEnd(CLASS_NAME, PerfLogger.PARTITION_RETRIEVING);
    return result;
  }

  /**
   * Pruning partition by getting the partition names first and pruning using Hive expression
   * evaluator on client.
   * @param tab the table containing the partitions.
   * @param partitions the resulting partitions.
   * @param prunerExpr the SQL predicate that involves partition columns.
   * @param conf Hive Configuration object, can not be NULL.
   * @return true iff the partition pruning expression contains non-partition columns.
   */
  static private boolean pruneBySequentialScan(Table tab, List<Partition> partitions,
      ExprNodeDesc prunerExpr, HiveConf conf) throws HiveException, MetaException {
    PerfLogger perfLogger = SessionState.getPerfLogger();
    perfLogger.perfLogBegin(CLASS_NAME, PerfLogger.PRUNE_LISTING);

    List<String> partNames = Hive.get().getPartitionNames(
        tab.getDbName(), tab.getTableName(), (short) -1);

    String defaultPartitionName = conf.getVar(HiveConf.ConfVars.DEFAULT_PARTITION_NAME);
    List<String> partCols = extractPartColNames(tab);
    List<PrimitiveTypeInfo> partColTypeInfos = extractPartColTypes(tab);

    boolean hasUnknownPartitions = prunePartitionNames(
        partCols, partColTypeInfos, prunerExpr, defaultPartitionName, partNames);
    perfLogger.perfLogEnd(CLASS_NAME, PerfLogger.PRUNE_LISTING);

    perfLogger.perfLogBegin(CLASS_NAME, PerfLogger.PARTITION_RETRIEVING);
    if (!partNames.isEmpty()) {
      partitions.addAll(Hive.get().getPartitionsByNames(tab, partNames));
    }
    perfLogger.perfLogEnd(CLASS_NAME, PerfLogger.PARTITION_RETRIEVING);
    return hasUnknownPartitions;
  }

  private static List<String> extractPartColNames(Table tab) {
    List<FieldSchema> pCols = tab.getPartCols();
    List<String> partCols = new ArrayList<String>(pCols.size());
    for (FieldSchema pCol : pCols) {
      partCols.add(pCol.getName());
    }
    return partCols;
  }

  private static List<PrimitiveTypeInfo> extractPartColTypes(Table tab) {
    List<FieldSchema> pCols = tab.getPartCols();
    List<PrimitiveTypeInfo> partColTypeInfos = new ArrayList<PrimitiveTypeInfo>(pCols.size());
    for (FieldSchema pCol : pCols) {
      partColTypeInfos.add(TypeInfoFactory.getPrimitiveTypeInfo(pCol.getType()));
    }
    return partColTypeInfos;
  }

  /**
   * Prunes partition names to see if they match the prune expression.
   * @param partColumnNames name of partition columns
   * @param partColumnTypeInfos types of partition columns
   * @param prunerExpr The expression to match.
   * @param defaultPartitionName name of default partition
   * @param partNames Partition names to filter. The list is modified in place.
   * @return Whether the list has any partitions for which the expression may or may not match.
   */
  public static boolean prunePartitionNames(List<String> partColumnNames,
      List<PrimitiveTypeInfo> partColumnTypeInfos, ExprNodeDesc prunerExpr,
      String defaultPartitionName, List<String> partNames) throws HiveException, MetaException {
    // Prepare the expression to filter on the columns.
    Pair<PrimitiveObjectInspector, ExprNodeEvaluator> handle =
        PartExprEvalUtils.prepareExpr(prunerExpr, partColumnNames, partColumnTypeInfos);

    // Filter the name list. Removing elements one by one can be slow on e.g. ArrayList,
    // so let's create a new list and copy it if we don't have a linked list
    boolean inPlace = partNames instanceof AbstractSequentialList<?>;
    List<String> partNamesSeq = inPlace ? partNames : new LinkedList<String>(partNames);

    // Array for the values to pass to evaluator.
    ArrayList<String> values = new ArrayList<String>(partColumnNames.size());
    for (int i = 0; i < partColumnNames.size(); ++i) {
      values.add(null);
    }

    boolean hasUnknownPartitions = false;
    Iterator<String> partIter = partNamesSeq.iterator();
    while (partIter.hasNext()) {
      String partName = partIter.next();
      Warehouse.makeValsFromName(partName, values);

      ArrayList<Object> convertedValues = new ArrayList<Object>(values.size());
      for(int i=0; i<values.size(); i++) {
        String partitionValue = values.get(i);
        PrimitiveTypeInfo typeInfo = partColumnTypeInfos.get(i);

        if (partitionValue.equals(defaultPartitionName)) {
          convertedValues.add(null); // Null for default partition.
        } else {
          Object o = ObjectInspectorConverters.getConverter(
              PrimitiveObjectInspectorFactory.javaStringObjectInspector,
              PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(typeInfo))
              .convert(partitionValue);
          convertedValues.add(o);
        }
      }

      // Evaluate the expression tree.
      Boolean isNeeded = (Boolean)PartExprEvalUtils.evaluateExprOnPart(handle, convertedValues);
      boolean isUnknown = (isNeeded == null);
      if (!isUnknown && !isNeeded) {
        partIter.remove();
        continue;
      }
      if (isUnknown && values.contains(defaultPartitionName)) {
        // Reject default partitions if we couldn't determine whether we should include it or not.
        // Note that predicate would only contains partition column parts of original predicate.
        LOG.debug("skipping default/bad partition: {}", partName);
        partIter.remove();
        continue;
      }
      hasUnknownPartitions |= isUnknown;
      LOG.debug("retained unknown:[{}] partition: {}", isUnknown, partName);

    }
    if (!inPlace) {
      partNames.clear();
      partNames.addAll(partNamesSeq);
    }
    return hasUnknownPartitions;
  }

  /**
   * Whether the expression contains a column node or not.
   */
  public static boolean hasColumnExpr(ExprNodeDesc desc) {
    // Return false for null
    if (desc == null) {
      return false;
    }
    // Return true for exprNodeColumnDesc
    if (desc instanceof ExprNodeColumnDesc) {
      return true;
    }
    // Return true in case one of the children is column expr.
    List<ExprNodeDesc> children = desc.getChildren();
    if (children != null) {
      for (int i = 0; i < children.size(); i++) {
        if (hasColumnExpr(children.get(i))) {
          return true;
        }
      }
    }
    // Return false otherwise
    return false;
  }

}
