package org.apache.hadoop.hive.ql.parse.repl.dump.events;

import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.ql.parse.repl.dump.Utils;

abstract class AbstractConstraintEventHandler extends AbstractEventHandler {
  AbstractConstraintEventHandler(NotificationEvent event) {
    super(event);
  }

  boolean shouldReplicate(Context withinContext) {
    return Utils.shouldReplicate(
        event,
        withinContext.replicationSpec,
        withinContext.db,
        withinContext.hiveConf
    );
  }
}
