package org.apache.hadoop.hive.metastore.events;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.metastore.api.SQLCheckConstraint;

import java.util.List;

@InterfaceAudience.Public
@InterfaceStability.Stable
public class AddCheckConstraintEvent extends ListenerEvent {
  private final List<SQLCheckConstraint> ds;

  public AddCheckConstraintEvent(List<SQLCheckConstraint> ds, boolean status,
                                   IHMSHandler handler) {
    super(status, handler);
    this.ds = ds;
  }

  public List<SQLCheckConstraint> getCheckConstraintCols() {
    return ds;
  }
}
