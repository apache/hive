package org.apache.hadoop.hive.ql.optimizer.physical;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.Stack;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.StatsTask;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.tez.DagUtils;
import org.apache.hadoop.hive.ql.exec.tez.TezTask;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.lib.TaskGraphWalker;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.MergeJoinWork;
import org.apache.hadoop.hive.ql.plan.ReduceWork;
import org.apache.hadoop.hive.ql.plan.TezWork;

/**
 * SerializeFilter is a simple physical optimizer that serializes all filter expressions in
 * Tablescan Operators.
 */
public class SerializeFilter implements PhysicalPlanResolver {

  protected static transient final Log LOG = LogFactory.getLog(SerializeFilter.class);

  public class Serializer implements Dispatcher {

    private final PhysicalContext pctx;

    public Serializer(PhysicalContext pctx) {
      this.pctx = pctx;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object dispatch(Node nd, Stack<Node> stack, Object... nodeOutputs)
      throws SemanticException {
      Task<? extends Serializable> currTask = (Task<? extends Serializable>) nd;
      if (currTask instanceof StatsTask) {
        currTask = ((StatsTask) currTask).getWork().getSourceTask();
      }
      if (currTask instanceof TezTask) {
        TezWork work = ((TezTask) currTask).getWork();
        for (BaseWork w : work.getAllWork()) {
          evaluateWork(w);
        }
      }
      return null;
    }

    private void evaluateWork(BaseWork w) throws SemanticException {

      if (w instanceof MapWork) {
        evaluateMapWork((MapWork) w);
      } else if (w instanceof ReduceWork) {
        evaluateReduceWork((ReduceWork) w);
      } else if (w instanceof MergeJoinWork) {
        evaluateMergeWork((MergeJoinWork) w);
      } else {
        LOG.info("We are not going to evaluate this work type: " + w.getClass().getCanonicalName());
      }
    }

    private void evaluateMergeWork(MergeJoinWork w) throws SemanticException {
      for (BaseWork baseWork : w.getBaseWorkList()) {
        evaluateOperators(baseWork, pctx);
      }
    }

    private void evaluateReduceWork(ReduceWork w) throws SemanticException {
      evaluateOperators(w, pctx);
    }

    private void evaluateMapWork(MapWork w) throws SemanticException {
      evaluateOperators(w, pctx);
    }

    private void evaluateOperators(BaseWork w, PhysicalContext pctx) throws SemanticException {

      Dispatcher disp = null;
      final Set<TableScanOperator> tableScans = new LinkedHashSet<TableScanOperator>();

      Map<Rule, NodeProcessor> rules = new HashMap<Rule, NodeProcessor>();
      rules.put(new RuleRegExp("TS finder",
              TableScanOperator.getOperatorName() + "%"), new NodeProcessor() {
          @Override
          public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
              Object... nodeOutputs) {
            tableScans.add((TableScanOperator) nd);
            return null;
          }
        });
      disp = new DefaultRuleDispatcher(null, rules, null);

      GraphWalker ogw = new DefaultGraphWalker(disp);

      ArrayList<Node> topNodes = new ArrayList<Node>();
      topNodes.addAll(w.getAllRootOperators());

      LinkedHashMap<Node, Object> nodeOutput = new LinkedHashMap<Node, Object>();
      ogw.startWalking(topNodes, nodeOutput);

      for (TableScanOperator ts: tableScans) {
        if (ts.getConf() != null && ts.getConf().getFilterExpr() != null) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Serializing: " + ts.getConf().getFilterExpr().getExprString());
          }
          ts.getConf().setSerializedFilterExpr(
            Utilities.serializeExpression(ts.getConf().getFilterExpr()));
        }

        if (ts.getConf() != null && ts.getConf().getFilterObject() != null) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Serializing: " + ts.getConf().getFilterObject());
          }

          ts.getConf().setSerializedFilterObject(
            Utilities.serializeObject(ts.getConf().getFilterObject()));
        }
      }
    }

    public class DefaultRule implements NodeProcessor {

      @Override
      public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
          Object... nodeOutputs) throws SemanticException {
        return null;
      }
    }
  }

  @Override
  public PhysicalContext resolve(PhysicalContext pctx) throws SemanticException {

    pctx.getConf();

    // create dispatcher and graph walker
    Dispatcher disp = new Serializer(pctx);
    TaskGraphWalker ogw = new TaskGraphWalker(disp);

    // get all the tasks nodes from root task
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pctx.getRootTasks());

    // begin to walk through the task tree.
    ogw.startWalking(topNodes, null);
    return pctx;
  }
}
