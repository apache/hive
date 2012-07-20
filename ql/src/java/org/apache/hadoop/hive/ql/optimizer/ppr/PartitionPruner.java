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
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
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
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.thrift.TException;

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

    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
    opRules.put(new RuleRegExp("R1", "(TS%FIL%)|(TS%FIL%FIL%)"), OpProcFactory
        .getFilterProc());

    // The dispatcher fires the processor corresponding to the closest matching
    // rule and passes the context along
    Dispatcher disp = new DefaultRuleDispatcher(OpProcFactory.getDefaultProc(),
        opRules, opWalkerCtx);
    GraphWalker ogw = new DefaultGraphWalker(disp);

    // Create a list of topop nodes
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pctx.getTopOps().values());
    ogw.startWalking(topNodes, null);
    pctx.setHasNonPartCols(opWalkerCtx.getHasNonPartCols());

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
   * @return the partition list for the table that satisfies the partition
   *         pruner condition.
   * @throws HiveException
   */
  public static PrunedPartitionList prune(Table tab, ExprNodeDesc prunerExpr,
      HiveConf conf, String alias,
      Map<String, PrunedPartitionList> prunedPartitionsMap) throws HiveException {
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

    LinkedHashSet<Partition> true_parts = new LinkedHashSet<Partition>();
    LinkedHashSet<Partition> unkn_parts = new LinkedHashSet<Partition>();
    LinkedHashSet<Partition> denied_parts = new LinkedHashSet<Partition>();

    try {
      StructObjectInspector rowObjectInspector = (StructObjectInspector) tab
          .getDeserializer().getObjectInspector();
      Object[] rowWithPart = new Object[2];

      if (tab.isPartitioned()) {
        // If the "strict" mode is on, we have to provide partition pruner for
        // each table.
        if ("strict".equalsIgnoreCase(HiveConf.getVar(conf,
            HiveConf.ConfVars.HIVEMAPREDMODE))) {
          if (!hasColumnExpr(prunerExpr)) {
            throw new SemanticException(ErrorMsg.NO_PARTITION_PREDICATE
                .getMsg("for Alias \"" + alias + "\" Table \""
                    + tab.getTableName() + "\""));
          }
        }

        if (prunerExpr == null) {
          // This can happen when hive.mapred.mode=nonstrict and there is no predicates at all
          // Add all partitions to the unknown_parts so that a MR job is generated.
          true_parts.addAll(Hive.get().getPartitions(tab));
        } else {
          // remove non-partition columns
          ExprNodeDesc compactExpr = prunerExpr.clone();
          compactExpr = compactExpr(compactExpr);
          LOG.debug("Filter w/ compacting: " +
              ((compactExpr != null) ? compactExpr.getExprString(): "null") +
              "; filter w/o compacting: " +
              ((prunerExpr != null) ? prunerExpr.getExprString(): "null"));
          if (compactExpr == null) {
            // This could happen when hive.mapred.mode=nonstrict and all the predicates
            // are on non-partition columns.
            unkn_parts.addAll(Hive.get().getPartitions(tab));
          } else if (Utilities.checkJDOPushDown(tab, compactExpr)) {
            String filter = compactExpr.getExprString();
            String oldFilter = prunerExpr.getExprString();

            if (filter.equals(oldFilter)) {
              // pruneExpr contains only partition columns
              pruneByPushDown(tab, true_parts, filter);
            } else {
              // pruneExpr contains non-partition columns
              pruneByPushDown(tab, unkn_parts, filter);
            }
          } else {
            pruneBySequentialScan(tab, true_parts, unkn_parts, denied_parts, prunerExpr, rowObjectInspector);
          }
        }
        LOG.debug("tabname = " + tab.getTableName() + " is partitioned");
      } else {
        true_parts.addAll(Hive.get().getPartitions(tab));
      }
    } catch (HiveException e) {
      throw e;
    } catch (Exception e) {
      throw new HiveException(e);
    }

    // Now return the set of partitions
    ret = new PrunedPartitionList(tab, true_parts, unkn_parts, denied_parts);
    prunedPartitionsMap.put(key, ret);
    return ret;
  }

  /**
   * Taking a partition pruning expression, remove the null operands.
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
      if (udf instanceof GenericUDFOPAnd ||
          udf instanceof GenericUDFOPOr) {
        List<ExprNodeDesc> children = ((ExprNodeGenericFuncDesc)expr).getChildren();
        ExprNodeDesc left = children.get(0);
        children.set(0, compactExpr(left));
        ExprNodeDesc right = children.get(1);
        children.set(1, compactExpr(right));
        if (children.get(0) == null && children.get(1) == null) {
          return null;
        } else if (children.get(0) == null) {
          return children.get(1);
        } else if (children.get(1) == null) {
          return children.get(0);
        }
      }
      return expr;
    }
    return expr;
  }

  /**
   * Pruning partition using JDO filtering.
   * @param tab the table containing the partitions.
   * @param true_parts the resulting partitions.
   * @param filter the SQL predicate that involves only partition columns
   * @throws HiveException
   * @throws MetaException
   * @throws NoSuchObjectException
   * @throws TException
   */
  static private void pruneByPushDown(Table tab, Set<Partition> true_parts, String filter)
      throws HiveException, MetaException, NoSuchObjectException, TException {
    Hive db = Hive.get();
    List<Partition> parts = db.getPartitionsByFilter(tab, filter);
    true_parts.addAll(parts);
    return;
  }

  /**
   * Pruning partition by getting the partition names first and pruning using Hive expression
   * evaluator.
   * @param tab the table containing the partitions.
   * @param true_parts the resulting partitions if the partition pruning expression only contains
   *        partition columns.
   * @param unkn_parts the resulting partitions if the partition pruning expression that only contains
   *        non-partition columns.
   * @param denied_parts pruned out partitions.
   * @param prunerExpr the SQL predicate that involves partition columns.
   * @param rowObjectInspector object inspector used by the evaluator
   * @throws Exception
   */
  static private void pruneBySequentialScan(Table tab, Set<Partition> true_parts, Set<Partition> unkn_parts,
      Set<Partition> denied_parts, ExprNodeDesc prunerExpr, StructObjectInspector rowObjectInspector)
      throws Exception {

    List<String> trueNames = null;
    List<String> unknNames = null;

    PerfLogger perfLogger = PerfLogger.getPerfLogger();

    perfLogger.PerfLogBegin(LOG, PerfLogger.PRUNE_LISTING);

    List<String> partNames = Hive.get().getPartitionNames(tab.getDbName(),
        tab.getTableName(), (short) -1);

    List<FieldSchema> pCols = tab.getPartCols();
    List<String> partCols = new ArrayList<String>(pCols.size());
    List<String> values = new ArrayList<String>(pCols.size());
    Object[] objectWithPart = new Object[2];

    for (FieldSchema pCol : pCols) {
      partCols.add(pCol.getName());
    }

    Map<PrimitiveObjectInspector, ExprNodeEvaluator> handle = PartExprEvalUtils.prepareExpr(
        prunerExpr, partCols, rowObjectInspector);

    for (String partName : partNames) {

      // Set all the variables here
      LinkedHashMap<String, String> partSpec = Warehouse
          .makeSpecFromName(partName);

      values.clear();
      for (Map.Entry<String, String> kv: partSpec.entrySet()) {
        values.add(kv.getValue());
      }
      objectWithPart[1] = values;

      // evaluate the expression tree
      Boolean r = (Boolean) PartExprEvalUtils.evaluateExprOnPart(handle, objectWithPart);

      if (r == null) {
        if (unknNames == null) {
          unknNames = new LinkedList<String>();
        }
        unknNames.add(partName);
        LOG.debug("retained unknown partition: " + partName);
      } else if (Boolean.TRUE.equals(r)) {
        if (trueNames == null) {
          trueNames = new LinkedList<String>();
        }
        trueNames.add(partName);
        LOG.debug("retained partition: " + partName);
      }
    }
    perfLogger.PerfLogEnd(LOG, PerfLogger.PRUNE_LISTING);

    perfLogger.PerfLogBegin(LOG, PerfLogger.PARTITION_RETRIEVING);
    if (trueNames != null) {
      List<Partition> parts = Hive.get().getPartitionsByNames(tab, trueNames);
      true_parts.addAll(parts);
    }
    if (unknNames != null) {
      List<Partition> parts = Hive.get().getPartitionsByNames(tab, unknNames);
      unkn_parts.addAll(parts);
    }
    perfLogger.PerfLogEnd(LOG, PerfLogger.PARTITION_RETRIEVING);
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
