package org.apache.hadoop.hive.metastore.messaging;

import org.apache.hadoop.hive.metastore.api.SQLDefaultConstraint;

import java.util.List;

public abstract class AddDefaultConstraintMessage extends EventMessage {
  protected AddDefaultConstraintMessage() {
    super(EventType.ADD_DEFAULTCONSTRAINT);
  }

  /**
   * Getter for list of default constraints.
   * @return List of SQLDefaultConstraint
   */
  public abstract List<SQLDefaultConstraint> getDefaultConstraints() throws Exception;
}
