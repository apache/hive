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
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.Transform;
import org.apache.hadoop.hive.ql.parse.ErrorMsg;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.PrunedPartitionList;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

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
    LOG.trace("tabname = " + tab.getTableName());
    LOG.trace("prune Expression = " + prunerExpr);

    String key = tab.getTableName() + ";";
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
        LOG.debug("tabname = " + tab.getTableName() + " is partitioned");

        for (String partName : Hive.get().getPartitionNames(tab.getDbName(),
            tab.getTableName(), (short) -1)) {
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

          // Set all the variables here
          LinkedHashMap<String, String> partSpec = Warehouse
              .makeSpecFromName(partName);

          LOG.trace("about to process partition " + partSpec + " for pruning ");
          // evaluate the expression tree
          if (prunerExpr != null) {
            Boolean r = (Boolean) PartExprEvalUtils.evalExprWithPart(prunerExpr, partSpec,
                rowObjectInspector);

            LOG.trace("prune result for partition " + partSpec + ": " + r);
            if (Boolean.FALSE.equals(r)) {
              if (denied_parts.isEmpty()) {
                Partition part = Hive.get().getPartition(tab, partSpec,
                    Boolean.FALSE);
                denied_parts.add(part);
              }
              LOG.trace("pruned partition: " + partSpec);
            } else {
              Partition part = Hive.get().getPartition(tab, partSpec,
                  Boolean.FALSE);
              String state = "retained";
              if (Boolean.TRUE.equals(r)) {
                true_parts.add(part);
              } else {
                unkn_parts.add(part);
                state = "unknown";
              }
              if (LOG.isDebugEnabled()) {
                LOG.debug(state + " partition: " + partSpec);
              }
            }
          } else {
            // is there is no parition pruning, all of them are needed
            true_parts.add(Hive.get()
                .getPartition(tab, partSpec, Boolean.FALSE));
          }
        }
      } else {
        true_parts.addAll(Hive.get().getPartitions(tab));
      }
    } catch (HiveException e) {
      throw e;
    } catch (Exception e) {
      throw new HiveException(e);
    }

    // Now return the set of partitions
    ret = new PrunedPartitionList(true_parts, unkn_parts, denied_parts);
    prunedPartitionsMap.put(key, ret);
    return ret;
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
