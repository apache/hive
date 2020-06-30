package org.apache.hadoop.hive.metastore.events;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.metastore.api.SQLDefaultConstraint;

import java.util.List;

@InterfaceAudience.Public
@InterfaceStability.Stable
public class AddDefaultConstraintEvent extends ListenerEvent {
  private final List<SQLDefaultConstraint> ds;

  public AddDefaultConstraintEvent(List<SQLDefaultConstraint> ds, boolean status,
                                   IHMSHandler handler) {
    super(status, handler);
    this.ds = ds;
  }

  public List<SQLDefaultConstraint> getDefaultConstraintCols() {
    return ds;
  }
}
