package org.apache.hadoop.hive.ql.optimizer.calcite.rules;

import java.util.ArrayList;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJdbcConverter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class JDBCAbstractSplitFilterRule extends RelOptRule {
  static Logger LOG = LoggerFactory.getLogger(JDBCAbstractSplitFilterRule.class);

  static public JDBCAbstractSplitFilterRule SPLIT_FILTER_ABOVE_JOIN = new JDBCSplitFilterAboveJoinRule ();
  static public JDBCAbstractSplitFilterRule SPLIT_FILTER_ABOVE_CONVERTER = new JDBCSplitFilterRule ();

  static public class FilterSupportedFunctionsVisitor extends RexVisitorImpl<Void> {

    private final SqlDialect dialect;

    public FilterSupportedFunctionsVisitor (SqlDialect dialect) {
      super (true);
      this.dialect = dialect;
    }

    final private ArrayList<RexCall> validJdbcNode = new ArrayList<RexCall> ();
    final private ArrayList<RexCall> invalidJdbcNode = new ArrayList<RexCall> ();

    public ArrayList<RexCall> getValidJdbcNode() {
      return validJdbcNode;
    }


    public ArrayList<RexCall> getInvalidJdbcNode() {
      return invalidJdbcNode;
    }

    @Override
    public Void visitCall(RexCall call) {
      if (call.getKind() == SqlKind.AND) {
        return super.visitCall(call);
      } else {
        boolean isValidCall = JDBCRexCallValidator.isValidJdbcOperation(call, dialect);
        if (isValidCall) {
          validJdbcNode.add(call);
        } else {
          invalidJdbcNode.add(call);
        }
      }
      return null;
    }

    public boolean canBeSplitted () {
       return !validJdbcNode.isEmpty() && !invalidJdbcNode.isEmpty();
    }
  }

  protected JDBCAbstractSplitFilterRule (RelOptRuleOperand operand) {
    super (operand);
  }

  static public boolean canSplitFilter(RexNode cond, SqlDialect dialect) {
    FilterSupportedFunctionsVisitor visitor = new FilterSupportedFunctionsVisitor(dialect);
    cond.accept(visitor);
    return visitor.canBeSplitted();
  }

  public boolean matches(RelOptRuleCall call, SqlDialect dialect) {
    LOG.debug("MySplitFilter.matches has been called");
    
    final HiveFilter filter = call.rel(0);
    
    RexNode cond = filter.getCondition ();

    return canSplitFilter(cond, dialect);
  }

  public void onMatch(RelOptRuleCall call, SqlDialect dialect) {
    LOG.debug("MySplitFilter.onMatch has been called");

    final HiveFilter        filter = call.rel(0);

    RexCall callExpression = (RexCall) filter.getCondition ();

    FilterSupportedFunctionsVisitor visitor = new FilterSupportedFunctionsVisitor(dialect);
    callExpression.accept(visitor);

    ArrayList<RexCall> validJdbcNode = visitor.getValidJdbcNode();
    ArrayList<RexCall> invalidJdbcNode = visitor.getInvalidJdbcNode();

    assert validJdbcNode.size() != 0 && invalidJdbcNode.size() != 0;

    final RexBuilder rexBuilder = filter.getCluster().getRexBuilder();

    RexNode validCondition;
    if (validJdbcNode.size() == 1) {
      validCondition = validJdbcNode.get(0);
    } else {
      validCondition = rexBuilder.makeCall(SqlStdOperatorTable.AND, validJdbcNode);
    }

    HiveFilter newJdbcValidFilter = new HiveFilter(filter.getCluster(), filter.getTraitSet(), filter.getInput(), validCondition);

    RexNode invalidCondition;
    if (invalidJdbcNode.size() == 1) {
      invalidCondition = invalidJdbcNode.get(0);
    } else {
      invalidCondition = rexBuilder.makeCall(SqlStdOperatorTable.AND, invalidJdbcNode);
    }

    HiveFilter newJdbcInvalidFilter = new HiveFilter(filter.getCluster(), filter.getTraitSet(), newJdbcValidFilter, invalidCondition);

    call.transformTo(newJdbcInvalidFilter);
  }

  public static class JDBCSplitFilterAboveJoinRule extends JDBCAbstractSplitFilterRule {
    public JDBCSplitFilterAboveJoinRule() {
      super(operand(HiveFilter.class,
              operand(HiveJoin.class, 
                  operand(HiveJdbcConverter.class, any()))));
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
      LOG.debug("MyUpperJoinFilterFilter.matches has been called");

      final HiveJoin join = call.rel(1);
      final HiveJdbcConverter conv = call.rel(2);

      RexNode joinCond = join.getCondition ();

      return super.matches(call) && JDBCRexCallValidator.isValidJdbcOperation(joinCond, conv.getJdbcDialect());
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      final HiveJdbcConverter conv = call.rel(0);
      super.onMatch(call, conv.getJdbcDialect());
    }
  }

  public static class JDBCSplitFilterRule extends JDBCAbstractSplitFilterRule {
    public JDBCSplitFilterRule() {
      super(operand(HiveFilter.class,
              operand(HiveJdbcConverter.class, any())));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      final HiveJdbcConverter conv = call.rel(1);
      super.onMatch(call, conv.getJdbcDialect());
    }
  }

};