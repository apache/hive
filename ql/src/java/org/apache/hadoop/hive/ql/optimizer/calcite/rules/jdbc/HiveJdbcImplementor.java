package org.apache.hadoop.hive.ql.optimizer.calcite.rules.jdbc;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.adapter.jdbc.JdbcImplementor;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;

public class HiveJdbcImplementor extends JdbcImplementor {

  public HiveJdbcImplementor(SqlDialect dialect, JavaTypeFactory typeFactory) {
    super(dialect, typeFactory);
  }

  public Result visit(Project e) {
    // This variant keeps the column aliases instead of producing STAR
    Result x = visitChild(0, e.getInput());
    parseCorrelTable(e, x);
    final Builder builder =
        x.builder(e, Clause.SELECT);
    final List<SqlNode> selectList = new ArrayList<>();
    for (RexNode ref : e.getChildExps()) {
      SqlNode sqlExpr = builder.context.toSql(null, ref);
      addSelect(selectList, sqlExpr, e.getRowType());
    }

    builder.setSelect(new SqlNodeList(selectList, POS));
    return builder.result();
  }

  public Result visit(Sort e) {
    Result x = visitChild(0, e.getInput());
    Builder builder = x.builder(e, Clause.ORDER_BY);
    List<SqlNode> orderByList = Expressions.list();
    for (RelFieldCollation field : e.getCollation().getFieldCollations()) {
      builder.addOrderItem(orderByList, field);
    }

    // Create select list as we want to keep the column aliases
    // instead of producing STAR
    final List<SqlNode> selectList = new ArrayList<>();
    for (int i = 0; i < e.getRowType().getFieldCount(); i++) {
      RexInputRef ref = RexInputRef.of(i, e.getRowType());
      SqlNode sqlExpr = builder.context.toSql(null, ref);
      addSelect(selectList, sqlExpr, e.getRowType());
    }
    builder.setSelect(new SqlNodeList(selectList, POS));

    if (!orderByList.isEmpty()) {
      builder.setOrderBy(new SqlNodeList(orderByList, POS));
      x = builder.result();
    }
    if (e.fetch != null) {
      builder = x.builder(e, Clause.FETCH);
      builder.setFetch(builder.context.toSql(null, e.fetch));
      x = builder.result();
    }
    if (e.offset != null) {
      builder = x.builder(e, Clause.OFFSET);
      builder.setOffset(builder.context.toSql(null, e.offset));
      x = builder.result();
    }
    return x;
  }

  private void parseCorrelTable(RelNode relNode, Result x) {
    for (CorrelationId id : relNode.getVariablesSet()) {
      correlTableMap.put(id, x.qualifiedContext());
    }
  }

  public Result implement(RelNode node) {
    return dispatch(node);
  }
}
