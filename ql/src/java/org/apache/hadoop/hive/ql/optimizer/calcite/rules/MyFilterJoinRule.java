package org.apache.hadoop.hive.ql.optimizer.calcite.rules;

import java.util.List;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.RelFactories.FilterFactory;
import org.apache.calcite.rel.core.RelFactories.ProjectFactory;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJdbcConverter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;

/**
 * Rule that tries to push filter expressions into a join condition and into
 * the inputs of the join.
 */
public class MyFilterJoinRule extends HiveFilterJoinRule {
  public MyFilterJoinRule() {
    super(RelOptRule.operand(HiveFilter.class, 
            RelOptRule.operand(HiveJoin.class, 
                RelOptRule.operand(HiveJdbcConverter.class, RelOptRule.any()),
                RelOptRule.operand(HiveJdbcConverter.class, RelOptRule.any()))),
        "MyFilterJoinRule", true, HiveRelFactories.HIVE_BUILDER);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    Filter filter = call.rel(0);
    Join   join = call.rel(1);
    boolean visitorRes = MyJdbcRexCallValidator.isValidJdbcOperation(filter.getCondition());
    if (visitorRes) {
      return MyJdbcRexCallValidator.isValidJdbcOperation(join.getCondition());  
    }
    return false;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Filter filter = call.rel(0);
    Join join = call.rel(1);
    super.perform(call, filter, join);
  }
}
