package org.apache.hadoop.hive.ql.plan;

import java.io.Serializable;

import org.apache.hadoop.hive.metastore.api.WMPool;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

@Explain(displayName = "Create/Alter Pool", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class CreateOrAlterWMPoolDesc extends DDLDesc implements Serializable {
  private static final long serialVersionUID = 4872940135771213510L;

  private WMPool pool;
  private String poolPath;
  private boolean update;

  public CreateOrAlterWMPoolDesc() {}

  public CreateOrAlterWMPoolDesc(WMPool pool, String poolPath, boolean update) {
    this.pool = pool;
    this.poolPath = poolPath;
    this.update = update;
  }

  @Explain(displayName="pool", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public WMPool getPool() {
    return pool;
  }

  public void setPool(WMPool pool) {
    this.pool = pool;
  }

  @Explain(displayName="poolPath", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getPoolPath() {
    return poolPath;
  }

  public void setPoolPath(String poolPath) {
    this.poolPath = poolPath;
  }

  @Explain(displayName="isUpdate", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public boolean isUpdate() {
    return update;
  }

  public void setUpdate(boolean update) {
    this.update = update;
  }
}
