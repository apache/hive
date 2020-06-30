package org.apache.hadoop.hive.metastore.messaging;

import org.apache.hadoop.hive.metastore.api.SQLCheckConstraint;

import java.util.List;

public abstract class AddCheckConstraintMessage extends EventMessage {
  protected AddCheckConstraintMessage() {
    super(EventType.ADD_CHECKCONSTRAINT);
  }

  /**
   * Getter for list of check constraints.
   * @return List of SQLCheckConstraint
   */
  public abstract List<SQLCheckConstraint> getCheckConstraints() throws Exception;
}
