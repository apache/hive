package org.apache.hadoop.hive.ql.optimizer.calcite.rules;

import java.util.Arrays;

import org.apache.calcite.adapter.jdbc.JdbcRules.JdbcProject;
import org.apache.calcite.adapter.jdbc.JdbcRules.JdbcProjectRule;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexNode;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJdbcConverter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JDBCProjectPushDownRule extends RelOptRule {
  static Logger LOG = LoggerFactory.getLogger(JDBCProjectPushDownRule.class);
  
  public static final JDBCProjectPushDownRule INSTANCE = new JDBCProjectPushDownRule ();
  
  public JDBCProjectPushDownRule() {
    super(operand(HiveProject.class,
            operand(HiveJdbcConverter.class, any())));
  }
  
  @Override
  public boolean matches(RelOptRuleCall call) {
    final HiveProject project = call.rel(0);
    final HiveJdbcConverter conv = call.rel(1);
    for (RexNode curr_project : project.getProjects()) {
        if (MyJdbcRexCallValidator.isValidJdbcOperation(curr_project, conv.getJdbcDialect()) == false) {
          return false;
        }
    }

    return true;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    LOG.debug("MyProjectPushDownRule has been called");
    
    final HiveProject project = call.rel(0);
    final HiveJdbcConverter converter = call.rel(1);
    
    Project newHiveProject = project.copy(project.getTraitSet(), converter.getInput(),project.getProjects(), project.getRowType());
    JdbcProject newJdbcProject = (JdbcProject) new JdbcProjectRule(converter.getJdbcConvention()).convert(newHiveProject);
    if (newJdbcProject != null) {
      RelNode ConverterRes = converter.copy(converter.getTraitSet(), Arrays.asList(newJdbcProject));
      call.transformTo(ConverterRes);
    }
  }
  
};