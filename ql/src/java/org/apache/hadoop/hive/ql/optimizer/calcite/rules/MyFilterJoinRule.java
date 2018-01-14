package org.apache.hadoop.hive.ql.optimizer.calcite.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJdbcConverter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;

/**
 * Rule that tries to push filter expressions into a join condition and into
 * the inputs of the join.
 */
public class MyFilterJoinRule extends HiveFilterJoinRule {
  
  final static public MyFilterJoinRule INSTANCE = new MyFilterJoinRule ();
  
  
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
