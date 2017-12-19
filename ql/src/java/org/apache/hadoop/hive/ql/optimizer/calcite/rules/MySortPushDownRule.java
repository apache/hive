package org.apache.hadoop.hive.ql.optimizer.calcite.rules;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.adapter.jdbc.JdbcRules.JdbcSort;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexNode;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSortLimit;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJdbcConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySortPushDownRule extends RelOptRule {
  static Logger LOG = LoggerFactory.getLogger(MySortPushDownRule.class);
  
  public MySortPushDownRule() {
    super(operand(HiveSortLimit.class,
        operand(HiveJdbcConverter.class, operand(RelNode.class, any()))));
  }
  
  public boolean matches(RelOptRuleCall call) {
    final Sort sort = (Sort) call.rel(0);
    
    
    for (RexNode curr_call : sort.getChildExps()) {
      if (MyJdbcRexCallValidator.isValidJdbcOperation(curr_call) == false) {
        return false;
      }
    }
    
    return true;
  }
  
  private JdbcSort convert(RelNode rel, JdbcConvention out) {
    final Sort sort = (Sort) rel;
    //if (sort.offset != null || sort.fetch != null) {
    //  // Cannot implement "OFFSET n FETCH n" currently.
    //  return null;
    //}
    final RelTraitSet traitSet = sort.getTraitSet().replace(out);
    return new JdbcSort(rel.getCluster(), traitSet,
        sort.getInput(), sort.getCollation());
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    LOG.debug("MySortPushDownRule has been called");
    
    final HiveSortLimit sort = call.rel(0);
    final HiveJdbcConverter converter = call.rel(1);
    final RelNode input = call.rel(2);
    
    
    //FIXME TODOY not working!!!!!!
    
    Sort newHiveSort = sort.copy(sort.getTraitSet(), input, sort.getCollation() ,sort.getOffsetExpr (),sort.getFetchExpr());
    JdbcSort newJdbcSort = convert (newHiveSort, JdbcConvention.JETHRO_DEFAULT_CONVENTION);
    if (newJdbcSort != null) {
      RelNode ConverterRes = converter.copy(converter.getTraitSet(), Arrays.asList(newJdbcSort));
      
      call.transformTo(ConverterRes);
    }
  }
  
};