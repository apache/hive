package org.apache.hadoop.hive.ql.optimizer.calcite.rules;

import java.util.Arrays;

import org.apache.calcite.adapter.jdbc.JdbcRules.JdbcFilter;
import org.apache.calcite.adapter.jdbc.JdbcRules.JdbcFilterRule;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexNode;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJdbcConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JDBCFilterPushDownRule extends RelOptRule {
  static Logger LOG = LoggerFactory.getLogger(JDBCFilterPushDownRule.class);
  
  public static final JDBCFilterPushDownRule INSTANCE = new JDBCFilterPushDownRule ();

  public JDBCFilterPushDownRule() {
    super(operand(HiveFilter.class,
            operand(HiveJdbcConverter.class, any())));
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    final HiveFilter filter = call.rel(0);
    final HiveJdbcConverter converter = call.rel(1);
    
    RexNode cond = filter.getCondition ();

    return JDBCRexCallValidator.isValidJdbcOperation(cond, converter.getJdbcDialect());
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    LOG.debug("MyFilterPushDown has been called");

    final HiveFilter filter = call.rel(0);
    final HiveJdbcConverter converter = call.rel(1);

    Filter newHiveFilter = filter.copy(filter.getTraitSet(), converter.getInput(),filter.getCondition());
    JdbcFilter newJdbcFilter = (JdbcFilter) new JdbcFilterRule(converter.getJdbcConvention()).convert(newHiveFilter);
    if (newJdbcFilter != null) {
      RelNode ConverterRes = converter.copy(converter.getTraitSet(), Arrays.asList(newJdbcFilter));

      call.transformTo(ConverterRes);
    }
  }
  
};