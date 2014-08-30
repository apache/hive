package org.apache.hadoop.hive.ql.optimizer.optiq.translator;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Stack;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.LimitOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
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
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.optiq.RelOptHiveTable;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveAggregateRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveFilterRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveJoinRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveProjectRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveSortRel;
import org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveTableScanRel;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.PrunedPartitionList;
import org.apache.hadoop.hive.ql.parse.QBJoinTree;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.ColStatistics;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.JoinCondDesc;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.Statistics;
import org.eigenbase.rel.AggregateCall;
import org.eigenbase.rel.Aggregation;
import org.eigenbase.rel.InvalidRelException;
import org.eigenbase.rel.JoinRelType;
import org.eigenbase.rel.RelCollation;
import org.eigenbase.rel.RelCollationImpl;
import org.eigenbase.rel.RelFieldCollation;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.TableAccessRelBase;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptSchema;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.rex.RexCall;
import org.eigenbase.rex.RexInputRef;
import org.eigenbase.rex.RexNode;
import org.eigenbase.rex.RexUtil;
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.util.CompositeList;
import org.eigenbase.util.Pair;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

public class RelNodeConverter {
  private static final Map<String, Aggregation> AGG_MAP = ImmutableMap
                                                            .<String, Aggregation> builder()
                                                            .put(
                                                                "count",
                                                                SqlStdOperatorTable.COUNT)
                                                            .put("sum", SqlStdOperatorTable.SUM)
                                                            .put("min", SqlStdOperatorTable.MIN)
                                                            .put("max", SqlStdOperatorTable.MAX)
                                                            .put("avg", SqlStdOperatorTable.AVG)
                                                            .build();

  public static RelNode convert(Operator<? extends OperatorDesc> sinkOp, RelOptCluster cluster,
      RelOptSchema schema, SemanticAnalyzer sA, ParseContext pCtx) {

    Context ctx = new Context(cluster, schema, sA, pCtx);

    Map<Rule, NodeProcessor> rules = ImmutableMap
        .<Rule, NodeProcessor> builder()
        .put(new RuleRegExp("R1", TableScanOperator.getOperatorName() + "%"),
            new TableScanProcessor())
        .put(new RuleRegExp("R2", FilterOperator.getOperatorName() + "%"), new FilterProcessor())
        .put(new RuleRegExp("R3", SelectOperator.getOperatorName() + "%"), new SelectProcessor())
        .put(new RuleRegExp("R4", JoinOperator.getOperatorName() + "%"), new JoinProcessor())
        .put(new RuleRegExp("R5", LimitOperator.getOperatorName() + "%"), new LimitProcessor())
        .put(new RuleRegExp("R6", GroupByOperator.getOperatorName() + "%"), new GroupByProcessor())
        .put(new RuleRegExp("R7", ReduceSinkOperator.getOperatorName() + "%"),
            new ReduceSinkProcessor()).build();

    Dispatcher disp = new DefaultRuleDispatcher(new DefaultProcessor(), rules, ctx);
    GraphWalker egw = new ForwardWalker(disp);

    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pCtx.getTopOps().values());

    HashMap<Node, Object> outputMap = new HashMap<Node, Object>();
    try {
      egw.startWalking(topNodes, outputMap);
    } catch (SemanticException se) {
      // @revisit
      throw new RuntimeException(se);
    }
    return (HiveRel) outputMap.get(sinkOp);
  }

  static class Context implements NodeProcessorCtx {
    RelOptCluster                                  cluster;
    RelOptSchema                                   schema;
    SemanticAnalyzer                               sA;
    ParseContext                                   parseCtx;
    /*
     * A Map from hive column internalNames to Optiq positions. A separate map
     * for each Operator.
     */
    Map<RelNode, ImmutableMap<String, Integer>>    opPositionMap;

    Map<Operator<? extends OperatorDesc>, RelNode> hiveOpToRelNode;

    public Context(RelOptCluster cluster, RelOptSchema schema, SemanticAnalyzer sA,
        ParseContext parseCtx) {
      super();
      this.cluster = cluster;
      this.schema = schema;
      this.sA = sA;
      this.parseCtx = parseCtx;
      opPositionMap = new HashMap<RelNode, ImmutableMap<String, Integer>>();
      hiveOpToRelNode = new HashMap<Operator<? extends OperatorDesc>, RelNode>();
    }

    void buildColumnMap(Operator<? extends OperatorDesc> op, RelNode rNode) {
      RowSchema rr = op.getSchema();
      ImmutableMap.Builder<String, Integer> b = new ImmutableMap.Builder<String, Integer>();
      int i = 0;
      for (ColumnInfo ci : rr.getSignature()) {
        b.put(ci.getInternalName(), i);
        i++;
      }
      opPositionMap.put(rNode, b.build());
    }

    /*
     * Why special handling for TableScan? - the RowResolver coming from hive
     * for TScan still has all the columns, whereas the Optiq type we build is
     * based on the needed columns in the TScanOp.
     */
    void buildColumnMap(TableScanOperator tsOp, RelNode rNode) {
      RelDataType oType = rNode.getRowType();
      int i = 0;
      ImmutableMap.Builder<String, Integer> b = new ImmutableMap.Builder<String, Integer>();
      for (String fN : oType.getFieldNames()) {
        b.put(fN, i);
        i++;
      }
      opPositionMap.put(rNode, b.build());
    }

    Map<String, Integer> reducerMap(Map<String, Integer> inpMap, ReduceSinkOperator rsOp) {
      ImmutableMap.Builder<String, Integer> b = new ImmutableMap.Builder<String, Integer>();
      Map<String, ExprNodeDesc> colExprMap = rsOp.getColumnExprMap();
      for (Map.Entry<String, ExprNodeDesc> e : colExprMap.entrySet()) {
        String inpCol = ((ExprNodeColumnDesc) e.getValue()).getColumn();
        b.put(e.getKey(), inpMap.get(inpCol));
      }
      return b.build();
    }

    /*
     * The Optiq JoinRel datatype is formed by combining the columns from its
     * input RelNodes. Whereas the Hive RowResolver of the JoinOp contains only
     * the columns needed by childOps.
     */
    void buildColumnMap(JoinOperator jOp, HiveJoinRel jRel) throws SemanticException {
      RowResolver rr = sA.getRowResolver(jOp);
      QBJoinTree hTree = parseCtx.getJoinContext().get(jOp);
      Map<String, Integer> leftMap = opPositionMap.get(jRel.getLeft());
      Map<String, Integer> rightMap = opPositionMap.get(jRel.getRight());
      leftMap = reducerMap(leftMap, (ReduceSinkOperator) jOp.getParentOperators().get(0));
      rightMap = reducerMap(rightMap, (ReduceSinkOperator) jOp.getParentOperators().get(1));
      int leftColCount = jRel.getLeft().getRowType().getFieldCount();
      ImmutableMap.Builder<String, Integer> b = new ImmutableMap.Builder<String, Integer>();
      for (Map.Entry<String, LinkedHashMap<String, ColumnInfo>> tableEntry : rr.getRslvMap()
          .entrySet()) {
        String table = tableEntry.getKey();
        LinkedHashMap<String, ColumnInfo> cols = tableEntry.getValue();
        Map<String, Integer> posMap = leftMap;
        int offset = 0;
        if (hTree.getRightAliases() != null) {
          for (String rAlias : hTree.getRightAliases()) {
            if (table.equals(rAlias)) {
              posMap = rightMap;
              offset = leftColCount;
              break;
            }
          }
        }
        for (Map.Entry<String, ColumnInfo> colEntry : cols.entrySet()) {
          ColumnInfo ci = colEntry.getValue();
          ExprNodeDesc e = jOp.getColumnExprMap().get(ci.getInternalName());
          String cName = ((ExprNodeColumnDesc) e).getColumn();
          int pos = posMap.get(cName);

          b.put(ci.getInternalName(), pos + offset);
        }
      }
      opPositionMap.put(jRel, b.build());
    }

    void propagatePosMap(RelNode node, RelNode parent) {
      opPositionMap.put(node, opPositionMap.get(parent));
    }

    RexNode convertToOptiqExpr(final ExprNodeDesc expr, final RelNode optiqOP, final boolean flatten)
        throws SemanticException {
      return convertToOptiqExpr(expr, optiqOP, 0, flatten);
    }

    RexNode convertToOptiqExpr(final ExprNodeDesc expr, final RelNode optiqOP, int offset,
        final boolean flatten) throws SemanticException {
      ImmutableMap<String, Integer> posMap = opPositionMap.get(optiqOP);
      RexNodeConverter c = new RexNodeConverter(cluster, optiqOP.getRowType(), posMap, offset,
          flatten);
      return c.convert(expr);
    }

    RelNode getParentNode(Operator<? extends OperatorDesc> hiveOp, int i) {
      Operator<? extends OperatorDesc> p = hiveOp.getParentOperators().get(i);
      return p == null ? null : hiveOpToRelNode.get(p);
    }

  }

  static class JoinProcessor implements NodeProcessor {
    @Override
    @SuppressWarnings("unchecked")
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      Context ctx = (Context) procCtx;
      HiveRel left = (HiveRel) ctx.getParentNode((Operator<? extends OperatorDesc>) nd, 0);
      HiveRel right = (HiveRel) ctx.getParentNode((Operator<? extends OperatorDesc>) nd, 1);
      JoinOperator joinOp = (JoinOperator) nd;
      JoinCondDesc[] jConds = joinOp.getConf().getConds();
      assert jConds.length == 1;
      HiveJoinRel joinRel = convertJoinOp(ctx, joinOp, jConds[0], left, right);
      ctx.buildColumnMap(joinOp, joinRel);
      ctx.hiveOpToRelNode.put(joinOp, joinRel);
      return joinRel;
    }

    /*
     * @todo: cleanup, for now just copied from HiveToOptiqRelConvereter
     */
    private HiveJoinRel convertJoinOp(Context ctx, JoinOperator op, JoinCondDesc jc,
        HiveRel leftRel, HiveRel rightRel) throws SemanticException {
      HiveJoinRel joinRel;
      Operator<? extends OperatorDesc> leftParent = op.getParentOperators().get(jc.getLeft());
      Operator<? extends OperatorDesc> rightParent = op.getParentOperators().get(jc.getRight());

      if (leftParent instanceof ReduceSinkOperator && rightParent instanceof ReduceSinkOperator) {
        List<ExprNodeDesc> leftCols = ((ReduceSinkDesc) (leftParent.getConf())).getKeyCols();
        List<ExprNodeDesc> rightCols = ((ReduceSinkDesc) (rightParent.getConf())).getKeyCols();
        RexNode joinPredicate = null;
        JoinRelType joinType = JoinRelType.INNER;
        int rightColOffSet = leftRel.getRowType().getFieldCount();

        // TODO: what about semi join
        switch (jc.getType()) {
        case JoinDesc.INNER_JOIN:
          joinType = JoinRelType.INNER;
          break;
        case JoinDesc.LEFT_OUTER_JOIN:
          joinType = JoinRelType.LEFT;
          break;
        case JoinDesc.RIGHT_OUTER_JOIN:
          joinType = JoinRelType.RIGHT;
          break;
        case JoinDesc.FULL_OUTER_JOIN:
          joinType = JoinRelType.FULL;
          break;
        }

        int i = 0;
        for (ExprNodeDesc expr : leftCols) {
          List<RexNode> eqExpr = new LinkedList<RexNode>();
          eqExpr.add(ctx.convertToOptiqExpr(expr, leftRel, 0, false));
          eqExpr.add(ctx.convertToOptiqExpr(rightCols.get(i), rightRel, rightColOffSet, false));

          RexNode eqOp = ctx.cluster.getRexBuilder().makeCall(SqlStdOperatorTable.EQUALS, eqExpr);
          i++;

          if (joinPredicate == null) {
            joinPredicate = eqOp;
          } else {
            List<RexNode> conjElements = new LinkedList<RexNode>();
            conjElements.add(joinPredicate);
            conjElements.add(eqOp);
            joinPredicate = ctx.cluster.getRexBuilder().makeCall(SqlStdOperatorTable.AND,
                conjElements);
          }
        }

        // Translate non-joinkey predicate
        Set<Entry<Byte, List<ExprNodeDesc>>> filterExprSet = op.getConf().getFilters().entrySet();
        if (!filterExprSet.isEmpty()) {
          RexNode eqExpr;
          int colOffSet;
          RelNode childRel;
          Operator parentHiveOp;
          int inputId;

          for (Entry<Byte, List<ExprNodeDesc>> entry : filterExprSet) {
            inputId = entry.getKey().intValue();
            if (inputId == 0) {
              colOffSet = 0;
              childRel = leftRel;
              parentHiveOp = leftParent;
            } else if (inputId == 1) {
              colOffSet = rightColOffSet;
              childRel = rightRel;
              parentHiveOp = rightParent;
            } else {
              throw new RuntimeException("Invalid Join Input");
            }

            for (ExprNodeDesc expr : entry.getValue()) {
              eqExpr = ctx.convertToOptiqExpr(expr, childRel, colOffSet, false);
              List<RexNode> conjElements = new LinkedList<RexNode>();
              conjElements.add(joinPredicate);
              conjElements.add(eqExpr);
              joinPredicate = ctx.cluster.getRexBuilder().makeCall(SqlStdOperatorTable.AND,
                  conjElements);
            }
          }
        }

        joinRel = HiveJoinRel.getJoin(ctx.cluster, leftRel, rightRel, joinPredicate, joinType,
            false);
      } else {
        throw new RuntimeException("Right & Left of Join Condition columns are not equal");
      }

      return joinRel;
    }

  }

  private static int convertExpr(Context ctx, RelNode input, ExprNodeDesc expr,
      List<RexNode> extraExprs) throws SemanticException {
    final RexNode rex = ctx.convertToOptiqExpr(expr, input, false);
    final int index;
    if (rex instanceof RexInputRef) {
      index = ((RexInputRef) rex).getIndex();
    } else {
      index = input.getRowType().getFieldCount() + extraExprs.size();
      extraExprs.add(rex);
    }
    return index;
  }

  private static AggregateCall convertAgg(Context ctx, AggregationDesc agg, RelNode input,
      ColumnInfo cI, List<RexNode> extraExprs) throws SemanticException {
    final Aggregation aggregation = AGG_MAP.get(agg.getGenericUDAFName());
    if (aggregation == null) {
      throw new AssertionError("agg not found: " + agg.getGenericUDAFName());
    }

    List<Integer> argList = new ArrayList<Integer>();
    RelDataType type = TypeConverter.convert(cI.getType(), ctx.cluster.getTypeFactory());
    if (aggregation.equals(SqlStdOperatorTable.AVG)) {
      type = type.getField("sum", false).getType();
    }
    for (ExprNodeDesc expr : agg.getParameters()) {
      int index = convertExpr(ctx, input, expr, extraExprs);
      argList.add(index);
    }

    /*
     * set the type to the first arg, it there is one; because the RTi set on
     * Aggregation call assumes this is the output type.
     */
    if (argList.size() > 0) {
      RexNode rex = ctx.convertToOptiqExpr(agg.getParameters().get(0), input, false);
      type = rex.getType();
    }
    return new AggregateCall(aggregation, agg.getDistinct(), argList, type, null);
  }

  static class FilterProcessor implements NodeProcessor {
    @Override
    @SuppressWarnings("unchecked")
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      Context ctx = (Context) procCtx;
      HiveRel input = (HiveRel) ctx.getParentNode((Operator<? extends OperatorDesc>) nd, 0);
      FilterOperator filterOp = (FilterOperator) nd;
      RexNode convertedFilterExpr = ctx.convertToOptiqExpr(filterOp.getConf().getPredicate(),
          input, true);

      // Flatten the condition otherwise Optiq chokes on assertion
      // (FilterRelBase)
      if (convertedFilterExpr instanceof RexCall) {
        RexCall call = (RexCall) convertedFilterExpr;
        convertedFilterExpr = ctx.cluster.getRexBuilder().makeCall(call.getType(),
            call.getOperator(), RexUtil.flatten(call.getOperands(), call.getOperator()));
      }

      HiveRel filtRel = new HiveFilterRel(ctx.cluster, ctx.cluster.traitSetOf(HiveRel.CONVENTION),
          input, convertedFilterExpr);
      ctx.propagatePosMap(filtRel, input);
      ctx.hiveOpToRelNode.put(filterOp, filtRel);
      return filtRel;
    }
  }

  static class SelectProcessor implements NodeProcessor {
    @Override
    @SuppressWarnings("unchecked")
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      Context ctx = (Context) procCtx;
      HiveRel inputRelNode = (HiveRel) ctx.getParentNode((Operator<? extends OperatorDesc>) nd, 0);
      SelectOperator selectOp = (SelectOperator) nd;

      List<ExprNodeDesc> colLst = selectOp.getConf().getColList();
      List<RexNode> optiqColLst = new LinkedList<RexNode>();

      for (ExprNodeDesc colExpr : colLst) {
        optiqColLst.add(ctx.convertToOptiqExpr(colExpr, inputRelNode, false));
      }

      /*
       * Hive treats names that start with '_c' as internalNames; so change the
       * names so we don't run into this issue when converting back to Hive AST.
       */
      List<String> oFieldNames = Lists.transform(selectOp.getConf().getOutputColumnNames(),
          new Function<String, String>() {
            @Override
            public String apply(String hName) {
              return "_o_" + hName;
            }
          });

      HiveRel selRel = HiveProjectRel.create(inputRelNode, optiqColLst, oFieldNames);
      ctx.buildColumnMap(selectOp, selRel);
      ctx.hiveOpToRelNode.put(selectOp, selRel);
      return selRel;
    }
  }

  static class LimitProcessor implements NodeProcessor {
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      Context ctx = (Context) procCtx;
      HiveRel input = (HiveRel) ctx.getParentNode((Operator<? extends OperatorDesc>) nd, 0);
      LimitOperator limitOp = (LimitOperator) nd;

      // in Optiq, a limit is represented as a sort on 0 columns
      final RexNode fetch;
      if (limitOp.getConf().getLimit() >= 0) {
        fetch = ctx.cluster.getRexBuilder().makeExactLiteral(
            BigDecimal.valueOf(limitOp.getConf().getLimit()));
      } else {
        fetch = null;
      }
      RelTraitSet traitSet = ctx.cluster.traitSetOf(HiveRel.CONVENTION);
      RelCollation canonizedCollation = traitSet.canonize(RelCollationImpl.EMPTY);
      HiveRel sortRel = new HiveSortRel(ctx.cluster, traitSet, input, canonizedCollation, null,
          fetch);
      ctx.propagatePosMap(sortRel, input);
      ctx.hiveOpToRelNode.put(limitOp, sortRel);
      return sortRel;
    }
  }

  static class GroupByProcessor implements NodeProcessor {
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      Context ctx = (Context) procCtx;

      HiveRel input = (HiveRel) ctx.getParentNode((Operator<? extends OperatorDesc>) nd, 0);
      GroupByOperator groupByOp = (GroupByOperator) nd;
      RowResolver rr = ctx.sA.getRowResolver(groupByOp);
      ArrayList<ColumnInfo> signature = rr.getRowSchema().getSignature();

      // GroupBy is represented by two operators, one map side and one reduce
      // side. We only translate the map-side one.
      if (groupByOp.getParentOperators().get(0) instanceof ReduceSinkOperator) {
        ctx.hiveOpToRelNode.put(groupByOp, input);
        return input;
      }

      final List<RexNode> extraExprs = Lists.newArrayList();
      final BitSet groupSet = new BitSet();
      for (ExprNodeDesc key : groupByOp.getConf().getKeys()) {
        int index = convertExpr(ctx, input, key, extraExprs);
        groupSet.set(index);
      }
      List<AggregateCall> aggregateCalls = Lists.newArrayList();
      int i = groupByOp.getConf().getKeys().size();
      for (AggregationDesc agg : groupByOp.getConf().getAggregators()) {
        aggregateCalls.add(convertAgg(ctx, agg, input, signature.get(i++), extraExprs));
      }

      if (!extraExprs.isEmpty()) {
        // noinspection unchecked
        input = HiveProjectRel.create(input, CompositeList.of(Lists.transform(input.getRowType()
            .getFieldList(), new Function<RelDataTypeField, RexNode>() {
          @Override
          public RexNode apply(RelDataTypeField input) {
            return new RexInputRef(input.getIndex(), input.getType());
          }
        }), extraExprs), null);
      }
      try {
        HiveRel aggregateRel = new HiveAggregateRel(ctx.cluster,
            ctx.cluster.traitSetOf(HiveRel.CONVENTION), input, groupSet, aggregateCalls);
        ctx.buildColumnMap(groupByOp, aggregateRel);
        ctx.hiveOpToRelNode.put(groupByOp, aggregateRel);
        return aggregateRel;
      } catch (InvalidRelException e) {
        throw new AssertionError(e); // not possible
      }
    }
  }

  static class ReduceSinkProcessor implements NodeProcessor {
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      Context ctx = (Context) procCtx;
      HiveRel input = (HiveRel) ctx.getParentNode((Operator<? extends OperatorDesc>) nd, 0);
      ReduceSinkOperator sinkOp = (ReduceSinkOperator) nd;

      // It is a sort reducer if and only if the number of reducers is 1.
      final ReduceSinkDesc conf = sinkOp.getConf();
      if (conf.getNumReducers() != 1) {
        Operator<? extends OperatorDesc> op = (Operator<? extends OperatorDesc>) nd;
        ctx.hiveOpToRelNode.put(op, input);
        return input;
      }

      final String order = conf.getOrder(); // "+-" means "ASC, DESC"
      assert order.length() == conf.getKeyCols().size();

      /*
       * numReducers == 1 and order.length = 1 => a RS for CrossJoin.
       */
      if (order.length() == 0) {
        Operator<? extends OperatorDesc> op = (Operator<? extends OperatorDesc>) nd;
        ctx.hiveOpToRelNode.put(op, input);
        return input;
      }

      final List<RelFieldCollation> fieldCollations = Lists.newArrayList();
      final List<RexNode> extraExprs = Lists.newArrayList();
      for (Pair<ExprNodeDesc, Character> pair : Pair.zip(conf.getKeyCols(),
          Lists.charactersOf(order))) {
        int index = convertExpr(ctx, input, pair.left, extraExprs);
        RelFieldCollation.Direction direction = getDirection(pair.right);
        fieldCollations.add(new RelFieldCollation(index, direction));
      }

      if (!extraExprs.isEmpty()) {
        // noinspection unchecked
        input = HiveProjectRel.create(input, CompositeList.of(Lists.transform(input.getRowType()
            .getFieldList(), new Function<RelDataTypeField, RexNode>() {
          @Override
          public RexNode apply(RelDataTypeField input) {
            return new RexInputRef(input.getIndex(), input.getType());
          }
        }), extraExprs), null);
      }

      RelTraitSet traitSet = ctx.cluster.traitSetOf(HiveRel.CONVENTION);
      RelCollation canonizedCollation = traitSet.canonize(RelCollationImpl.of(fieldCollations));
      HiveRel sortRel = new HiveSortRel(ctx.cluster, traitSet, input, canonizedCollation, null,
          null);
      ctx.propagatePosMap(sortRel, input);
      ctx.hiveOpToRelNode.put(sinkOp, sortRel);

      // REVIEW: Do we need to remove the columns we added due to extraExprs?

      return sortRel;
    }

    private RelFieldCollation.Direction getDirection(char c) {
      switch (c) {
      case '+':
        return RelFieldCollation.Direction.ASCENDING;
      case '-':
        return RelFieldCollation.Direction.DESCENDING;
      default:
        throw new AssertionError("unexpected direction " + c);
      }
    }
  }

  static class TableScanProcessor implements NodeProcessor {
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      Context ctx = (Context) procCtx;
      TableScanOperator tableScanOp = (TableScanOperator) nd;
      RowResolver rr = ctx.sA.getRowResolver(tableScanOp);

      List<String> neededCols = new ArrayList<String>(tableScanOp.getNeededColumns());
      Statistics stats = tableScanOp.getStatistics();

      try {
        stats = addPartitionColumns(ctx, tableScanOp, tableScanOp.getConf().getAlias(),
            ctx.sA.getTable(tableScanOp), stats, neededCols);
      } catch (CloneNotSupportedException ce) {
        throw new SemanticException(ce);
      }

      if (stats.getColumnStats().size() != neededCols.size()) {
        throw new SemanticException("Incomplete Col stats for table: "
            + tableScanOp.getConf().getAlias());
      }
      RelDataType rowType = TypeConverter.getType(ctx.cluster, rr, neededCols);
      RelOptHiveTable optTable = new RelOptHiveTable(ctx.schema, tableScanOp.getConf().getAlias(),
          rowType, ctx.sA.getTable(tableScanOp), null, null, null, null, null);
      TableAccessRelBase tableRel = new HiveTableScanRel(ctx.cluster,
          ctx.cluster.traitSetOf(HiveRel.CONVENTION), optTable, rowType);
      ctx.buildColumnMap(tableScanOp, tableRel);
      ctx.hiveOpToRelNode.put(tableScanOp, tableRel);
      return tableRel;
    }

    /*
     * Add partition columns to needed columns and fake the COlStats for it.
     */
    private Statistics addPartitionColumns(Context ctx, TableScanOperator tableScanOp,
        String tblAlias, Table tbl, Statistics stats, List<String> neededCols)
        throws CloneNotSupportedException {
      if (!tbl.isPartitioned()) {
        return stats;
      }
      List<ColStatistics> pStats = new ArrayList<ColStatistics>();
      List<FieldSchema> pCols = tbl.getPartCols();
      for (FieldSchema pC : pCols) {
        neededCols.add(pC.getName());
        ColStatistics cStats = stats.getColumnStatisticsForColumn(tblAlias, pC.getName());
        if (cStats == null) {
          PrunedPartitionList partList = ctx.parseCtx.getOpToPartList().get(tableScanOp);
          cStats = new ColStatistics(tblAlias, pC.getName(), pC.getType());
          cStats.setCountDistint(partList.getPartitions().size());
          pStats.add(cStats);
        }
      }
      if (pStats.size() > 0) {
        stats = stats.clone();
        stats.addToColumnStats(pStats);
      }

      return stats;
    }
  }

  static class DefaultProcessor implements NodeProcessor {
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      @SuppressWarnings("unchecked")
      Operator<? extends OperatorDesc> op = (Operator<? extends OperatorDesc>) nd;
      Context ctx = (Context) procCtx;
      RelNode node = ctx.getParentNode(op, 0);
      ctx.hiveOpToRelNode.put(op, node);
      return node;
    }
  }
}
