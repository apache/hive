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

package org.apache.hadoop.hive.ql.optimizer.ppr;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
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
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPOr;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;

/**
 * The transformation step that does partition pruning.
 *
 */
public class PartitionPruner implements Transform {

  // The log
  private static final Log LOG = LogFactory
    .getLog("hive.ql.optimizer.ppr.PartitionPruner");

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
        && !FunctionRegistry.isDeterministic(((ExprNodeGenericFuncDesc) expr)
        .getGenericUDF())) {
      return false;
    }

    // All columns of the expression must be parttioned columns
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
      String alias) throws HiveException {
    return prune(parseCtx.getTopToTable().get(ts), parseCtx.getOpToPartPruner().get(ts),
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
   * @throws HiveException
   */
  private static PrunedPartitionList prune(Table tab, ExprNodeDesc prunerExpr,
      HiveConf conf, String alias, Map<String, PrunedPartitionList> prunedPartitionsMap)
          throws HiveException {
    LOG.trace("Started pruning partiton");
    LOG.trace("dbname = " + tab.getDbName());
    LOG.trace("tabname = " + tab.getTableName());
    LOG.trace("prune Expression = " + prunerExpr);

    String key = tab.getDbName() + "." + tab.getTableName() + ";";

    if (prunerExpr != null) {
      key = key + prunerExpr.getExprString();
    }
    PrunedPartitionList ret = prunedPartitionsMap.get(key);
    if (ret != null) {
      return ret;
    }

    ret = getPartitionsFromServer(tab, prunerExpr, conf, alias);
    prunedPartitionsMap.put(key, ret);
    return ret;
  }

  /**
   * Taking a partition pruning expression, remove the null operands and non-partition columns.
   * The reason why there are null operands is ExprProcFactory classes, for example
   * PPRColumnExprProcessor.
   * @param expr original partition pruning expression.
   * @return partition pruning expression that only contains partition columns.
   */
  static private ExprNodeDesc compactExpr(ExprNodeDesc expr) {
    if (expr instanceof ExprNodeConstantDesc) {
      if (((ExprNodeConstantDesc)expr).getValue() == null) {
        return null;
      } else {
        return expr;
      }
    } else if (expr instanceof ExprNodeGenericFuncDesc) {
      GenericUDF udf = ((ExprNodeGenericFuncDesc)expr).getGenericUDF();
      boolean isAnd = udf instanceof GenericUDFOPAnd;
      if (isAnd || udf instanceof GenericUDFOPOr) {
        List<ExprNodeDesc> children = expr.getChildren();
        ExprNodeDesc left = children.get(0);
        children.set(0, compactExpr(left));
        ExprNodeDesc right = children.get(1);
        children.set(1, compactExpr(right));
        // Note that one does not simply compact (not-null or null) to not-null.
        // Only if we have an "and" is it valid to send one side to metastore.
        if (children.get(0) == null && children.get(1) == null) {
          return null;
        } else if (children.get(0) == null) {
          return isAnd ? children.get(1) : null;
        } else if (children.get(1) == null) {
          return isAnd ? children.get(0) : null;
        }
      }
      return expr;
    }
    return expr;
  }

  /**
   * See compactExpr. Some things in the expr are replaced with nulls for pruner, however
   * the virtual columns are not removed (ExprNodeColumnDesc cannot tell them apart from
   * partition columns), so we do it here.
   * The expression is only used to prune by partition name, so we have no business with VCs.
   * @param expr original partition pruning expression.
   * @param partCols list of partition columns for the table.
   * @return partition pruning expression that only contains partition columns from the list.
   */
  static private ExprNodeDesc removeNonPartCols(ExprNodeDesc expr, List<String> partCols) {
    if (expr instanceof ExprNodeColumnDesc
        && !partCols.contains(((ExprNodeColumnDesc) expr).getColumn())) {
      // Column doesn't appear to be a partition column for the table.
      return new ExprNodeConstantDesc(expr.getTypeInfo(), null);
    }
    if (expr instanceof ExprNodeGenericFuncDesc) {
      List<ExprNodeDesc> children = expr.getChildren();
      for (int i = 0; i < children.size(); ++i) {
        children.set(i, removeNonPartCols(children.get(i), partCols));
      }
    }
    return expr;
  }

  private static PrunedPartitionList getPartitionsFromServer(Table tab,
      ExprNodeDesc prunerExpr, HiveConf conf, String alias) throws HiveException {
    try {
      if (!tab.isPartitioned()) {
        // If the table is not partitioned, return everything.
        return new PrunedPartitionList(tab, Hive.get().getAllPartitionsForPruner(tab), false);
      }
      LOG.debug("tabname = " + tab.getTableName() + " is partitioned");

      if ("strict".equalsIgnoreCase(HiveConf.getVar(conf, HiveConf.ConfVars.HIVEMAPREDMODE))
          && !hasColumnExpr(prunerExpr)) {
        // If the "strict" mode is on, we have to provide partition pruner for each table.
        throw new SemanticException(ErrorMsg.NO_PARTITION_PREDICATE
            .getMsg("for Alias \"" + alias + "\" Table \"" + tab.getTableName() + "\""));
      }

      if (prunerExpr == null) {
        // This can happen when hive.mapred.mode=nonstrict and there is no predicates at all.
        return new PrunedPartitionList(tab, Hive.get().getAllPartitionsForPruner(tab), false);
      }

      // Remove virtual columns. See javadoc for details.
      prunerExpr = removeNonPartCols(prunerExpr, extractPartColNames(tab));
      // Remove all unknown parts e.g. non-partition columns. See javadoc for details.
      ExprNodeDesc compactExpr = compactExpr(prunerExpr.clone());
      String oldFilter = prunerExpr.getExprString();
      if (compactExpr == null) {
        // This could happen when hive.mapred.mode=nonstrict and all the predicates
        // are on non-partition columns.
        LOG.debug("Filter " + oldFilter + " was null after compacting");
        return new PrunedPartitionList(tab, Hive.get().getAllPartitionsForPruner(tab), true);
      }

      Set<Partition> partitions = new LinkedHashSet<Partition>();
      boolean hasUnknownPartitions = false;
      String message = Utilities.checkJDOPushDown(tab, compactExpr, null);
      if (message != null) {
        LOG.info(ErrorMsg.INVALID_JDO_FILTER_EXPRESSION.getMsg("by condition '"
            + message + "'"));
        hasUnknownPartitions = pruneBySequentialScan(tab, partitions, prunerExpr, conf);
      } else {
        String filter = compactExpr.getExprString();
        LOG.debug("Filter w/ compacting: " + filter +"; filter w/o compacting: " + oldFilter);
        hasUnknownPartitions = !filter.equals(oldFilter);
        partitions.addAll(Hive.get().getPartitionsByFilter(tab, filter));
      }
      return new PrunedPartitionList(tab, partitions, hasUnknownPartitions);
    } catch (HiveException e) {
      throw e;
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  /**
   * Pruning partition by getting the partition names first and pruning using Hive expression
   * evaluator.
   * @param tab the table containing the partitions.
   * @param partitions the resulting partitions.
   * @param prunerExpr the SQL predicate that involves partition columns.
   * @param conf Hive Configuration object, can not be NULL.
   * @return true iff the partition pruning expression contains non-partition columns.
   */
  static private boolean pruneBySequentialScan(Table tab, Set<Partition> partitions,
      ExprNodeDesc prunerExpr, HiveConf conf) throws Exception {
    PerfLogger perfLogger = PerfLogger.getPerfLogger();
    perfLogger.PerfLogBegin(LOG, PerfLogger.PRUNE_LISTING);

    List<String> partNames = Hive.get().getPartitionNames(
        tab.getDbName(), tab.getTableName(), (short) -1);

    String defaultPartitionName = conf.getVar(HiveConf.ConfVars.DEFAULTPARTITIONNAME);
    List<String> partCols = extractPartColNames(tab);

    boolean hasUnknownPartitions = prunePartitionNames(
        partCols, prunerExpr, defaultPartitionName, partNames);
    perfLogger.PerfLogEnd(LOG, PerfLogger.PRUNE_LISTING);

    perfLogger.PerfLogBegin(LOG, PerfLogger.PARTITION_RETRIEVING);
    if (!partNames.isEmpty()) {
      partitions.addAll(Hive.get().getPartitionsByNames(tab, partNames));
    }
    perfLogger.PerfLogEnd(LOG, PerfLogger.PARTITION_RETRIEVING);
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

  /**
   * Prunes partition names to see if they match the prune expression.
   * @param columnNames name of partition columns
   * @param prunerExpr The expression to match.
   * @param defaultPartitionName name of default partition
   * @param partNames Partition names to filter. The list is modified in place.
   * @return Whether the list has any partitions for which the expression may or may not match.
   */
  public static boolean prunePartitionNames(List<String> columnNames, ExprNodeDesc prunerExpr,
      String defaultPartitionName, List<String> partNames) throws HiveException, MetaException {
    // Prepare the expression to filter on the columns.
    ObjectPair<PrimitiveObjectInspector, ExprNodeEvaluator> handle =
        PartExprEvalUtils.prepareExpr(prunerExpr, columnNames);

    // Filter the name list.
    List<String> values = new ArrayList<String>(columnNames.size());
    boolean hasUnknownPartitions = false;
    Iterator<String> partIter = partNames.iterator();
    while (partIter.hasNext()) {
      String partName = partIter.next();
      LinkedHashMap<String, String> partSpec = Warehouse.makeSpecFromName(partName);
      values.clear();
      values.addAll(partSpec.values());

      // Evaluate the expression tree.
      Boolean isNeeded = (Boolean)PartExprEvalUtils.evaluateExprOnPart(handle, values);
      boolean isUnknown = (isNeeded == null);
      if (!isUnknown && !isNeeded) {
        partIter.remove();
        continue;
      }
      if (isUnknown && values.contains(defaultPartitionName)) {
        // Reject default partitions if we couldn't determine whether we should include it or not.
        // Note that predicate would only contains partition column parts of original predicate.
        LOG.debug("skipping default/bad partition: " + partName);
        partIter.remove();
        continue;
      }
      hasUnknownPartitions |= isUnknown;
      LOG.debug("retained " + (isUnknown ? "unknown " : "") + "partition: " + partName);
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
