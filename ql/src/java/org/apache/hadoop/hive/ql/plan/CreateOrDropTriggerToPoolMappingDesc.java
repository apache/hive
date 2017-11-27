package org.apache.hadoop.hive.ql.plan;

import java.io.Serializable;

import org.apache.hadoop.hive.ql.plan.Explain.Level;

@Explain(displayName = "Create/Drop Trigger to pool mappings",
    explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class CreateOrDropTriggerToPoolMappingDesc extends DDLDesc implements Serializable {
  private static final long serialVersionUID = 383046258694558029L;

  private String resourcePlanName;
  private String triggerName;
  private String poolPath;
  private boolean drop;

  public CreateOrDropTriggerToPoolMappingDesc() {}

  public CreateOrDropTriggerToPoolMappingDesc(String resourcePlanName, String triggerName,
      String poolPath, boolean drop) {
    this.resourcePlanName = resourcePlanName;
    this.triggerName = triggerName;
    this.poolPath = poolPath;
    this.drop = drop;
  }

  @Explain(displayName = "resourcePlanName",
      explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getResourcePlanName() {
    return resourcePlanName;
  }

  public void setResourcePlanName(String resourcePlanName) {
    this.resourcePlanName = resourcePlanName;
  }

  @Explain(displayName = "triggerName",
      explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getTriggerName() {
    return triggerName;
  }

  public void setTriggerName(String triggerName) {
    this.triggerName = triggerName;
  }

  @Explain(displayName = "poolPath",
      explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getPoolPath() {
    return poolPath;
  }

  public void setPoolPath(String poolPath) {
    this.poolPath = poolPath;
  }

  @Explain(displayName = "drop or create",
      explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public boolean shouldDrop() {
    return drop;
  }

  public void setDrop(boolean drop) {
    this.drop = drop;
  }
}
