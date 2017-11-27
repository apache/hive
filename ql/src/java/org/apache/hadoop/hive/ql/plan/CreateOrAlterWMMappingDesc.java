package org.apache.hadoop.hive.ql.plan;

import java.io.Serializable;

import org.apache.hadoop.hive.metastore.api.WMMapping;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

@Explain(displayName = "Create/Alter Mapping",
    explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class CreateOrAlterWMMappingDesc extends DDLDesc implements Serializable {
  private static final long serialVersionUID = -442968568922083053L;

  private WMMapping mapping;
  private boolean update;

  public CreateOrAlterWMMappingDesc() {}

  public CreateOrAlterWMMappingDesc(WMMapping mapping, boolean update) {
    this.mapping = mapping;
    this.update = update;
  }

  @Explain(displayName = "mapping", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public WMMapping getMapping() {
    return mapping;
  }

  public void setMapping(WMMapping mapping) {
    this.mapping = mapping;
  }

  @Explain(displayName = "update",
      explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public boolean isUpdate() {
    return update;
  }

  public void setUpdate(boolean update) {
    this.update = update;
  }
}
