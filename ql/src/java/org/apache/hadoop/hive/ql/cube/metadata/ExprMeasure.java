package org.apache.hadoop.hive.ql.cube.metadata;

import java.util.Map;

import org.apache.hadoop.hive.metastore.api.FieldSchema;

public final class ExprMeasure extends CubeMeasure {
  private final String expr;

  public ExprMeasure(FieldSchema column, String expr, String formatString,
      String aggregate, String unit) {
    super(column, formatString, aggregate, unit);
    this.expr = expr;
    assert (expr != null);
  }

  public ExprMeasure(FieldSchema column, String expr) {
    this(column, expr, null, null, null);
  }

  public ExprMeasure(String name, Map<String, String> props) {
    super(name, props);
    this.expr = props.get(MetastoreUtil.getMeasureExprPropertyKey(getName()));
  }

  public String getExpr() {
    return expr;
  }

  @Override
  public void addProperties(Map<String, String> props) {
    super.addProperties(props);
    props.put(MetastoreUtil.getMeasureExprPropertyKey(getName()), expr);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((getExpr() == null) ? 0 :
        getExpr().toLowerCase().hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }
    ExprMeasure other = (ExprMeasure) obj;
    if (this.getExpr() == null) {
      if (other.getExpr() != null) {
        return false;
      }
    } else if (!this.getExpr().equalsIgnoreCase(other.getExpr())) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    String str = super.toString();
    str += "expr:" + expr;
    return str;
  }
}
