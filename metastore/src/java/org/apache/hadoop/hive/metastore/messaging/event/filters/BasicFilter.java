package org.apache.hadoop.hive.metastore.messaging.event.filters;

import org.apache.hadoop.hive.metastore.IMetaStoreClient.NotificationFilter;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;

public abstract class BasicFilter implements NotificationFilter {
  @Override
  public boolean accept(final NotificationEvent event) {
    if (event == null) {
      return false; // get rid of trivial case first, so that we can safely assume non-null
    }
    return shouldAccept(event);
  }

  abstract boolean shouldAccept(final NotificationEvent event);
}
