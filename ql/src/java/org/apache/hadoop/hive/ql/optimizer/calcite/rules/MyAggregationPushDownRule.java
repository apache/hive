package org.apache.hadoop.hive.ql.optimizer.calcite.rules;

import java.util.Arrays;

import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.adapter.jdbc.JdbcRules.JdbcAggregate;
import org.apache.calcite.adapter.jdbc.JdbcRules.JdbcAggregateRule;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAggregate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJdbcConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MyAggregationPushDownRule extends RelOptRule {
  static Logger LOG = LoggerFactory.getLogger(MyAggregationPushDownRule.class);
  
  public MyAggregationPushDownRule() {
    super(operand(HiveAggregate.class,
            operand(HiveJdbcConverter.class, any())));
  }
  
  @Override
  public boolean matches(RelOptRuleCall call) {
    final HiveAggregate agg = call.rel(0);
    final HiveJdbcConverter converter = call.rel(1);
    
    for (AggregateCall relOptRuleOperand : agg.getAggCallList()) {
      
      SqlAggFunction f = relOptRuleOperand.getAggregation();
      SqlKind kind = f.getKind();
      if (converter.getJdbcConvention().dialect.supportsAggregateFunction(kind) == false) {
        return false;
      }
      
    };
    return true;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    LOG.debug("MyAggregationPushDownRule has been called");
    
    final HiveAggregate agg = call.rel(0);
    final HiveJdbcConverter converter = call.rel(1);
    //TODOY this is very naive imp, consult others!!!!!!
    
    Aggregate newHiveAggregate = agg.copy(agg.getTraitSet(), converter.getInput(),agg.getIndicatorCount() !=0,agg.getGroupSet(),agg.getGroupSets(),agg.getAggCallList());
    JdbcAggregate newJdbcAggregate = (JdbcAggregate) new JdbcAggregateRule(converter.getJdbcConvention()).convert(newHiveAggregate);
    if (newJdbcAggregate != null) {
      RelNode ConverterRes = converter.copy(converter.getTraitSet(), Arrays.asList(newJdbcAggregate));
      
      call.transformTo(ConverterRes);
    }
  }
  
};