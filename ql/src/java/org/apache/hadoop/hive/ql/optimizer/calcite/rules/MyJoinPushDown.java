package org.apache.hadoop.hive.ql.optimizer.calcite.rules;

import java.util.Arrays;

import org.apache.calcite.adapter.jdbc.JdbcRules.JdbcJoin;
import org.apache.calcite.adapter.jdbc.JdbcRules.JdbcJoinRule;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJdbcConverter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MyJoinPushDown extends RelOptRule {
  static Logger LOG = LoggerFactory.getLogger(MyJoinPushDown.class);
  
  public MyJoinPushDown() {
    super(operand(HiveJoin.class,
            operand(HiveJdbcConverter.class, any()),
            operand(HiveJdbcConverter.class, any())));
  }
  
  @Override
  public boolean matches(RelOptRuleCall call) {
    final HiveJoin join = call.rel(0);
    RexNode cond = join.getCondition();
    final HiveJdbcConverter converter1 = call.rel(1);
    final HiveJdbcConverter converter2 = call.rel(2);
    
    if (converter1.getJdbcConvention().dialect.equals(converter2.getJdbcConvention().dialect) == false) {
      return false;//TODOY ask
    }
    //TODOY
    //if (cond.getKind() == SqlKind.IS_TRUE) {
    //  return false;//We don't want to push cross join
    //}
    boolean visitorRes = MyJdbcRexCallValidator.isValidJdbcOperation(cond);
    return visitorRes;
  }
  
  @Override
  public void onMatch(RelOptRuleCall call) {
    LOG.debug("MyJoinPushDown has been called");
    
    final HiveJoin join = call.rel(0);
    final HiveJdbcConverter converter1 = call.rel(1);
    final HiveJdbcConverter converter2 = call.rel(2);
    
    RelNode input1 = converter1.getInput();
    RelNode input2 = converter2.getInput();
    
    HiveJoin newHiveJoin = join.copy(join.getTraitSet(), join.getCondition(), input1, input2, join.getJoinType(),join.isSemiJoinDone());
    JdbcJoin newJdbcJoin = (JdbcJoin) new JdbcJoinRule(converter1.getJdbcConvention()).convert(newHiveJoin, false);
    if (newJdbcJoin != null) {
      RelNode ConverterRes = converter1.copy(converter1.getTraitSet(), Arrays.asList(newJdbcJoin));
      if (ConverterRes != null) {
        call.transformTo(ConverterRes);
      } 
    }
  }
  
};