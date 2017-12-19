package org.apache.hadoop.hive.ql.optimizer.calcite.reloperators;

import java.util.List;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.adapter.jdbc.JdbcImplementor;
import org.apache.calcite.adapter.jdbc.JdbcRel;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.convert.ConverterImpl;
import org.apache.calcite.sql.SqlDialect;

public class HiveJdbcConverter extends ConverterImpl implements HiveRelNode {

  public HiveJdbcConverter(RelOptCluster cluster,  RelTraitSet traits,
      JdbcRel input) {
    super(cluster, ConventionTraitDef.INSTANCE, traits, input);
  }
  
  private HiveJdbcConverter(RelOptCluster cluster,  RelTraitSet traits,
      RelNode input) {
    super(cluster, ConventionTraitDef.INSTANCE, traits, input);
  }

  @Override
  public void implement(Implementor implementor) {

  }
  
  @Override
  public RelNode copy(
      RelTraitSet traitSet,
      List<RelNode> inputs) {
    //TODOY 1st: try use this(causes cast exception)return new HiveJdbcConverter(getCluster(), traitSet, (JdbcRel) sole(inputs));
    //TODOY 2st: return new HiveJdbcConverter(getCluster(), getTraitSet(), (JdbcRel) getInput());
    return new HiveJdbcConverter(getCluster(), traitSet, sole(inputs));
  }
  
  public String generateSql(SqlDialect dialect) {
    final JdbcImplementor jdbcImplementor =
        new JdbcImplementor(dialect,
            (JavaTypeFactory) getCluster().getTypeFactory());
    final JdbcImplementor.Result result =
        jdbcImplementor.visitChild(0, getInput());
    return result.asStatement().toSqlString(dialect).getSql();
  }
  
  public JdbcHiveTableScan getTableScan () {
    final  JdbcHiveTableScan []  tmpJdbcHiveTableScan = new JdbcHiveTableScan[1];
    new RelVisitor() {
      
      public void visit(
          RelNode node,
          int ordinal,
          RelNode parent) {
        if (node instanceof JdbcHiveTableScan && tmpJdbcHiveTableScan [0] == null) {
          tmpJdbcHiveTableScan [0] = (JdbcHiveTableScan) node;
        } else {
          super.visit(node, ordinal, parent);
        }
      }
    }.go(this);
    
    JdbcHiveTableScan jdbcHiveTableScan = tmpJdbcHiveTableScan [0];
    
    assert jdbcHiveTableScan != null;
    return jdbcHiveTableScan;
  }
  
  
  public JdbcConvention getUnderlyingConvention () {
    return (JdbcConvention) getTableScan().getConvention ();
  }

}
