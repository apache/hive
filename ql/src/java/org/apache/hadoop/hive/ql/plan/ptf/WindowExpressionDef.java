package org.apache.hadoop.hive.ql.plan.ptf;


public class WindowExpressionDef extends PTFExpressionDef {
  private String alias;

  public WindowExpressionDef() {}

  public WindowExpressionDef(PTFExpressionDef eDef) {
    super(eDef);
  }

  public String getAlias() {
    return alias;
  }

  public void setAlias(String alias) {
    this.alias = alias;
  }
}