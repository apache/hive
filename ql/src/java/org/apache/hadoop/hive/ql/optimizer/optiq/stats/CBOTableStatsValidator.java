package org.apache.hadoop.hive.ql.optimizer.optiq.stats;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Stack;

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.ForwardWalker;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.Statistics;

import com.google.common.collect.ImmutableMap;

public class CBOTableStatsValidator {
  private final CBOValidateStatsContext m_ctx = new CBOValidateStatsContext();

  public boolean validStats(Operator<? extends OperatorDesc> sinkOp, ParseContext pCtx) {
    Map<Rule, NodeProcessor> rules = ImmutableMap
        .<Rule, NodeProcessor> builder()
        .put(new RuleRegExp("R1", TableScanOperator.getOperatorName() + "%"),
            new TableScanProcessor()).build();

    Dispatcher disp = new DefaultRuleDispatcher(getDefaultProc(), rules, m_ctx);
    GraphWalker fWalker = new ForwardWalker(disp);

    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pCtx.getTopOps().values());

    try {
      fWalker.startWalking(topNodes, null);
    } catch (SemanticException e) {
      throw new RuntimeException(e);
    }

    return (m_ctx.m_tabsWithIncompleteStats.isEmpty());
  }

  public String getIncompleteStatsTabNames() {
    StringBuilder sb = new StringBuilder();
    for (String tabName : m_ctx.m_tabsWithIncompleteStats) {
      if (sb.length() > 1)
        sb.append(", ");
      sb.append(tabName);
    }
    return sb.toString();
  }

  private static NodeProcessor getDefaultProc() {
    return new NodeProcessor() {
      @Override
      public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
          Object... nodeOutputs) {
        return null;
        // TODO: Shouldn't we throw exception? as this would imply we got an op
        // tree with no TS
      }
    };
  }

  static class TableScanProcessor implements NodeProcessor {
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) {
      TableScanOperator tableScanOp = (TableScanOperator) nd;
      Statistics stats = tableScanOp.getStatistics();
      int noColsWithStats = (stats != null && stats.getColumnStats() != null) ? stats
          .getColumnStats().size() : 0;
      if (noColsWithStats != tableScanOp.getNeededColumns().size()) {
        ((CBOValidateStatsContext) procCtx).m_tabsWithIncompleteStats.add(tableScanOp.getConf()
            .getAlias());
      }
      return null;
    }
  }

  static class CBOValidateStatsContext implements NodeProcessorCtx {
    final private HashSet<String> m_tabsWithIncompleteStats = new HashSet<String>();
  }
}
