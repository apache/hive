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


/**
 * This is a designated RelNode that splits the Hive operators and the Jdbc operators,
 * every successor of this node will be Jdbc operator. 
 *
 */
public class HiveJdbcConverter extends ConverterImpl implements HiveRelNode {

  final private JdbcConvention _convention;
  
  public HiveJdbcConverter(RelOptCluster cluster,  RelTraitSet traits,
      JdbcRel input, JdbcConvention jc) {
    super(cluster, ConventionTraitDef.INSTANCE, traits, input);
    _convention = jc;
  }
  
  private HiveJdbcConverter(RelOptCluster cluster,  RelTraitSet traits,
      RelNode input, JdbcConvention jc) {
    super(cluster, ConventionTraitDef.INSTANCE, traits, input);
    _convention = jc;
  }
  
  public JdbcConvention getJdbcConvention () {
    return _convention;
  }

  public SqlDialect getJdbcDialect() {
    return _convention.dialect;
  }

  @Override
  public void implement(Implementor implementor) {

  }
  
  @Override
  public RelNode copy(
      RelTraitSet traitSet,
      List<RelNode> inputs) {
    return new HiveJdbcConverter(getCluster(), traitSet, sole(inputs), _convention);
  }
  
  public String generateSql() {
    SqlDialect dialect = getJdbcDialect();
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
