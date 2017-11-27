package org.apache.hadoop.hive.ql.plan;

import java.io.Serializable;

import org.apache.hadoop.hive.metastore.api.WMMapping;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

@Explain(displayName = "Drop resource plan",
    explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class DropWMMappingDesc extends DDLDesc implements Serializable {
  private static final long serialVersionUID = -1567558687529244218L;

  private WMMapping mapping;

  public DropWMMappingDesc() {}

  public DropWMMappingDesc(WMMapping mapping) {
    this.mapping = mapping;
  }

  @Explain(displayName = "mapping", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public WMMapping getMapping() {
    return mapping;
  }

  public void setMapping(WMMapping mapping) {
    this.mapping = mapping;
  }
}
