package org.apache.hadoop.hive.ql.plan;

import java.io.Serializable;

import org.apache.hadoop.hive.ql.plan.Explain.Level;

@Explain(displayName = "Drop Resource plans", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class DropResourcePlanDesc extends DDLDesc implements Serializable {
  private static final long serialVersionUID = 1258596919510047766L;

  private String rpName;

  public DropResourcePlanDesc() {}

  public DropResourcePlanDesc(String rpName) {
    this.setRpName(rpName);
  }

  @Explain(displayName="resourcePlanName", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getRpName() {
    return rpName;
  }

  public void setRpName(String rpName) {
    this.rpName = rpName;
  }

}