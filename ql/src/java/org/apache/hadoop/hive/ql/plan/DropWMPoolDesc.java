package org.apache.hadoop.hive.ql.plan;

import java.io.Serializable;

public class DropWMPoolDesc extends DDLDesc implements Serializable {
  private static final long serialVersionUID = -2608462103392563252L;

  private String resourcePlanName;
  private String poolPath;

  public DropWMPoolDesc() {}

  public DropWMPoolDesc(String resourcePlanName, String poolPath) {
    this.resourcePlanName = resourcePlanName;
    this.poolPath = poolPath;
  }

  public String getResourcePlanName() {
    return resourcePlanName;
  }

  public void setResourcePlanName(String resourcePlanName) {
    this.resourcePlanName = resourcePlanName;
  }

  public String getPoolPath() {
    return poolPath;
  }

  public void setPoolPath(String poolPath) {
    this.poolPath = poolPath;
  }
}
