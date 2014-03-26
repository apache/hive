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
package org.apache.hadoop.hive.ql.optimizer.physical;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.io.OneNullRowInputFormat;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.PreOrderWalker;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.NullStructSerDe;

/**
 *
 * MetadataOnlyOptimizer determines to which TableScanOperators "metadata only"
 * optimization can be applied. Such operator must use only partition columns
 * (it is easy to check, because we are after column pruning and all places
 * where the data from the operator is used must go through GroupByOperator
 * distinct or distinct-like aggregations. Aggregation is distinct-like if
 * adding distinct wouldn't change the result, for example min, max.
 *
 * We cannot apply the optimization without group by, because the results depend
 * on the numbers of rows in partitions, for example count(hr) will count all
 * rows in matching partitions.
 *
 */
public class MetadataOnlyOptimizer implements PhysicalPlanResolver {
  private static final Log LOG = LogFactory.getLog(MetadataOnlyOptimizer.class.getName());

  static private class WalkerCtx implements NodeProcessorCtx {
    /* operators for which there is chance the optimization can be applied */
    private final HashSet<TableScanOperator> possible = new HashSet<TableScanOperator>();
    /* operators for which the optimization will be successful */
    private final HashSet<TableScanOperator> success = new HashSet<TableScanOperator>();

    /**
     * Sets operator as one for which there is a chance to apply optimization
     *
     * @param op
     *          the operator
     */
    public void setMayBeMetadataOnly(TableScanOperator op) {
      possible.add(op);
    }

    /** Convert all possible operators to success */
    public void convertMetadataOnly() {
      success.addAll(possible);
      possible.clear();
    }

    /**
     * Convert all possible operators to banned
     */
    public void convertNotMetadataOnly() {
      possible.clear();
      success.clear();
    }

    /**
     * Returns HashSet of collected operators for which the optimization may be
     * applicable.
     */
    public HashSet<TableScanOperator> getMayBeMetadataOnlyTableScans() {
      return possible;
    }

    /**
     * Returns HashSet of collected operators for which the optimization is
     * applicable.
     */
    public HashSet<TableScanOperator> getMetadataOnlyTableScans() {
      return success;
    }

  }

  static private class TableScanProcessor implements NodeProcessor {
    public TableScanProcessor() {
    }

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      TableScanOperator node = (TableScanOperator) nd;
      TableScanOperator tsOp = (TableScanOperator) nd;
      WalkerCtx walkerCtx = (WalkerCtx) procCtx;
      List<Integer> colIDs = tsOp.getNeededColumnIDs();
      TableScanDesc desc = tsOp.getConf();
      boolean noColNeeded = (colIDs == null) || (colIDs.isEmpty());
      boolean noVCneeded = (desc == null) || (desc.getVirtualCols() == null)
                             || (desc.getVirtualCols().isEmpty());
      if (noColNeeded && noVCneeded) {
        walkerCtx.setMayBeMetadataOnly(tsOp);
      }
      return nd;
    }
  }

  static private class FileSinkProcessor implements NodeProcessor {
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      WalkerCtx walkerCtx = (WalkerCtx) procCtx;
      // There can be atmost one element eligible to be converted to
      // metadata only
      if ((walkerCtx.getMayBeMetadataOnlyTableScans().isEmpty())
          || (walkerCtx.getMayBeMetadataOnlyTableScans().size() > 1)) {
        return nd;
      }

      for (Node op : stack) {
        if (op instanceof GroupByOperator) {
          GroupByOperator gby = (GroupByOperator) op;
          if (!gby.getConf().isDistinctLike()) {
            // GroupBy not distinct like, disabling
            walkerCtx.convertNotMetadataOnly();
            return nd;
          }
        }
      }

      walkerCtx.convertMetadataOnly();
      return nd;
    }
  }

  @Override
  public PhysicalContext resolve(PhysicalContext pctx) throws SemanticException {
    Dispatcher disp = new MetadataOnlyTaskDispatcher(pctx);
    GraphWalker ogw = new DefaultGraphWalker(disp);
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pctx.getRootTasks());
    ogw.startWalking(topNodes, null);
    return pctx;
  }

  /**
   * Iterate over all tasks one-to-one and convert them to metadata only
   */
  class MetadataOnlyTaskDispatcher implements Dispatcher {

    private final PhysicalContext physicalContext;

    public MetadataOnlyTaskDispatcher(PhysicalContext context) {
      super();
      physicalContext = context;
    }

    private String getAliasForTableScanOperator(MapWork work,
        TableScanOperator tso) {

      for (Map.Entry<String, Operator<? extends OperatorDesc>> entry :
        work.getAliasToWork().entrySet()) {
        if (entry.getValue() == tso) {
          return entry.getKey();
        }
      }

      return null;
    }

    private PartitionDesc changePartitionToMetadataOnly(PartitionDesc desc) {
      if (desc != null) {
        desc.setInputFileFormatClass(OneNullRowInputFormat.class);
        desc.setOutputFileFormatClass(HiveIgnoreKeyTextOutputFormat.class);
        desc.getProperties().setProperty(serdeConstants.SERIALIZATION_LIB,
          NullStructSerDe.class.getName());
      }
      return desc;
    }

    private List<String> getPathsForAlias(MapWork work, String alias) {
      List<String> paths = new ArrayList<String>();

      for (Map.Entry<String, ArrayList<String>> entry : work.getPathToAliases().entrySet()) {
        if (entry.getValue().contains(alias)) {
          paths.add(entry.getKey());
        }
      }

      return paths;
    }

    private void processAlias(MapWork work, String alias) {
      // Change the alias partition desc
      PartitionDesc aliasPartn = work.getAliasToPartnInfo().get(alias);
      changePartitionToMetadataOnly(aliasPartn);

      List<String> paths = getPathsForAlias(work, alias);
      for (String path : paths) {
        PartitionDesc partDesc = work.getPathToPartitionInfo().get(path);
        PartitionDesc newPartition = changePartitionToMetadataOnly(partDesc);
        Path fakePath = new Path(physicalContext.getContext().getMRTmpPath()
            + newPartition.getTableName()
            + encode(newPartition.getPartSpec()));
        work.getPathToPartitionInfo().remove(path);
        work.getPathToPartitionInfo().put(fakePath.getName(), newPartition);
        ArrayList<String> aliases = work.getPathToAliases().remove(path);
        work.getPathToAliases().put(fakePath.getName(), aliases);
      }
    }

    // considered using URLEncoder, but it seemed too much
    private String encode(Map<String, String> partSpec) {
      return partSpec.toString().replaceAll("[:/#\\?]", "_");
    }

    @Override
    public Object dispatch(Node nd, Stack<Node> stack, Object... nodeOutputs)
        throws SemanticException {
      Task<? extends Serializable> task = (Task<? extends Serializable>) nd;

      Collection<Operator<? extends OperatorDesc>> topOperators
        = task.getTopOperators();
      if (topOperators.size() == 0) {
        return null;
      }

      LOG.info("Looking for table scans where optimization is applicable");
      // create a the context for walking operators
      ParseContext parseContext = physicalContext.getParseContext();
      WalkerCtx walkerCtx = new WalkerCtx();

      Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
      opRules.put(new RuleRegExp("R1",
        TableScanOperator.getOperatorName() + "%"),
        new TableScanProcessor());
      opRules.put(new RuleRegExp("R2",
        GroupByOperator.getOperatorName() + "%.*" + FileSinkOperator.getOperatorName() + "%"),
        new FileSinkProcessor());

      // The dispatcher fires the processor corresponding to the closest
      // matching rule and passes the context along
      Dispatcher disp = new DefaultRuleDispatcher(null, opRules, walkerCtx);
      GraphWalker ogw = new PreOrderWalker(disp);

      // Create a list of topOp nodes
      ArrayList<Node> topNodes = new ArrayList<Node>();
      // Get the top Nodes for this map-reduce task
      for (Operator<? extends OperatorDesc>
           workOperator : topOperators) {
        if (parseContext.getTopOps().values().contains(workOperator)) {
          topNodes.add(workOperator);
        }
      }

      if (task.getReducer() != null) {
        topNodes.add(task.getReducer());
      }

      ogw.startWalking(topNodes, null);

      LOG.info(String.format("Found %d metadata only table scans",
          walkerCtx.getMetadataOnlyTableScans().size()));
      Iterator<TableScanOperator> iterator
        = walkerCtx.getMetadataOnlyTableScans().iterator();

      while (iterator.hasNext()) {
        TableScanOperator tso = iterator.next();
        ((TableScanDesc)tso.getConf()).setIsMetadataOnly(true);
        MapWork work = ((MapredWork) task.getWork()).getMapWork();
        String alias = getAliasForTableScanOperator(work, tso);
        LOG.info("Metadata only table scan for " + alias);
        processAlias(work, alias);
      }

      return null;
    }
  }
}
