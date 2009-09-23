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
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluatorFactory;
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
import org.apache.hadoop.hive.ql.plan.exprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * The transformation step that does partition pruning.
 *
 */
public class PartitionPruner implements Transform {

  // The log
  private static final Log LOG = LogFactory.getLog("hive.ql.optimizer.ppr.PartitionPruner");

  /* (non-Javadoc)
   * @see org.apache.hadoop.hive.ql.optimizer.Transform#transform(org.apache.hadoop.hive.ql.parse.ParseContext)
   */
  @Override
  public ParseContext transform(ParseContext pctx) throws SemanticException {

    // create a the context for walking operators
    OpWalkerCtx opWalkerCtx = new OpWalkerCtx(pctx.getOpToPartPruner());

    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
    opRules.put(new RuleRegExp("R1", "(TS%FIL%)|(TS%FIL%FIL%)"),
                OpProcFactory.getFilterProc());

    // The dispatcher fires the processor corresponding to the closest matching rule and passes the context along
    Dispatcher disp = new DefaultRuleDispatcher(OpProcFactory.getDefaultProc(), opRules, opWalkerCtx);
    GraphWalker ogw = new DefaultGraphWalker(disp);

    // Create a list of topop nodes
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pctx.getTopOps().values());
    ogw.startWalking(topNodes, null);
    pctx.setHasNonPartCols(opWalkerCtx.getHasNonPartCols());

    return pctx;
  }

  /**
   * Find out whether the condition only contains partitioned columns. Note that if the table
   * is not partitioned, the function always returns true.
   * condition.
   *
   * @param tab    the table object
   * @param expr   the pruner expression for the table
   */
  public static boolean onlyContainsPartnCols(Table tab, exprNodeDesc expr) {
    if (!tab.isPartitioned() || (expr == null))
      return true;

    if (expr instanceof exprNodeColumnDesc) {
      String colName = ((exprNodeColumnDesc)expr).getColumn();
      return tab.isPartitionKey(colName);
    }

    // It cannot contain a non-deterministic function
    if ((expr instanceof exprNodeGenericFuncDesc) &&
        !FunctionRegistry.isDeterministic(((exprNodeGenericFuncDesc)expr).getGenericUDF()))
      return false;

    // All columns of the expression must be parttioned columns
    List<exprNodeDesc> children = expr.getChildren();
    if (children != null) {
      for (int i = 0; i < children.size(); i++) {
        if (!onlyContainsPartnCols(tab, children.get(i)))
          return false;
      }
    }

    return true;
  }

  /**
   * Get the partition list for the table that satisfies the partition pruner
   * condition.
   *
   * @param tab    the table object for the alias
   * @param prunerExpr  the pruner expression for the alias
   * @param conf   for checking whether "strict" mode is on.
   * @param alias  for generating error message only.
   * @return the partition list for the table that satisfies the partition pruner condition.
   * @throws HiveException
   */
  public static PrunedPartitionList prune(Table tab, exprNodeDesc prunerExpr,
      HiveConf conf, String alias) throws HiveException {
    LOG.trace("Started pruning partiton");
    LOG.trace("tabname = " + tab.getName());
    LOG.trace("prune Expression = " + prunerExpr);

    LinkedHashSet<Partition> true_parts = new LinkedHashSet<Partition>();
    LinkedHashSet<Partition> unkn_parts = new LinkedHashSet<Partition>();
    LinkedHashSet<Partition> denied_parts = new LinkedHashSet<Partition>();

    try {
      StructObjectInspector rowObjectInspector = (StructObjectInspector)tab.getDeserializer().getObjectInspector();
      Object[] rowWithPart = new Object[2];

      if(tab.isPartitioned()) {
        for(String partName: Hive.get().getPartitionNames(tab.getDbName(), tab.getName(), (short) -1)) {
          // Set all the variables here
          LinkedHashMap<String, String> partSpec = Warehouse.makeSpecFromName(partName);
          // Create the row object
          ArrayList<String> partNames = new ArrayList<String>();
          ArrayList<String> partValues = new ArrayList<String>();
          ArrayList<ObjectInspector> partObjectInspectors = new ArrayList<ObjectInspector>();
          for(Map.Entry<String,String>entry : partSpec.entrySet()) {
            partNames.add(entry.getKey());
            partValues.add(entry.getValue());
            partObjectInspectors.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
          }
          StructObjectInspector partObjectInspector = ObjectInspectorFactory.getStandardStructObjectInspector(partNames, partObjectInspectors);

          rowWithPart[1] = partValues;
          ArrayList<StructObjectInspector> ois = new ArrayList<StructObjectInspector>(2);
          ois.add(rowObjectInspector);
          ois.add(partObjectInspector);
          StructObjectInspector rowWithPartObjectInspector = ObjectInspectorFactory.getUnionStructObjectInspector(ois);

          // If the "strict" mode is on, we have to provide partition pruner for each table.
          if ("strict".equalsIgnoreCase(HiveConf.getVar(conf, HiveConf.ConfVars.HIVEMAPREDMODE))) {
            if (!hasColumnExpr(prunerExpr)) {
              throw new SemanticException(ErrorMsg.NO_PARTITION_PREDICATE.getMsg(
                  "for Alias \"" + alias + "\" Table \"" + tab.getName() + "\""));
            }
          }

          // evaluate the expression tree
          if (prunerExpr != null) {
            ExprNodeEvaluator evaluator = ExprNodeEvaluatorFactory.get(prunerExpr);
            ObjectInspector evaluateResultOI = evaluator.initialize(rowWithPartObjectInspector);
            Object evaluateResultO = evaluator.evaluate(rowWithPart);
            Boolean r = (Boolean) ((PrimitiveObjectInspector)evaluateResultOI).getPrimitiveJavaObject(evaluateResultO);
            LOG.trace("prune result for partition " + partSpec + ": " + r);
            if (Boolean.FALSE.equals(r)) {
              if (denied_parts.isEmpty()) {
                Partition part = Hive.get().getPartition(tab, partSpec, Boolean.FALSE);
                denied_parts.add(part);
              }
              LOG.trace("pruned partition: " + partSpec);
            } else {
              Partition part = Hive.get().getPartition(tab, partSpec, Boolean.FALSE);
              if (Boolean.TRUE.equals(r)) {
                LOG.debug("retained partition: " + partSpec);
                true_parts.add(part);
              } else {
                LOG.debug("unknown partition: " + partSpec);
                unkn_parts.add(part);
              }
            }
          } else {
            // is there is no parition pruning, all of them are needed
            true_parts.add(Hive.get().getPartition(tab, partSpec, Boolean.FALSE));
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
    return new PrunedPartitionList(true_parts, unkn_parts, denied_parts);
  }

  /**
   * Whether the expression contains a column node or not.
   */
  public static boolean hasColumnExpr(exprNodeDesc desc) {
    // Return false for null
    if (desc == null) {
      return false;
    }
    // Return true for exprNodeColumnDesc
    if (desc instanceof exprNodeColumnDesc) {
      return true;
    }
    // Return true in case one of the children is column expr.
    List<exprNodeDesc> children = desc.getChildren();
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
