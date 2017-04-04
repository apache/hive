package org.apache.hadoop.hive.metastore.messaging.event.filters;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;

public class AndFilter implements IMetaStoreClient.NotificationFilter {
  final IMetaStoreClient.NotificationFilter[] filters;

  public AndFilter(final IMetaStoreClient.NotificationFilter... filters) {
    this.filters = filters;
  }

  @Override
  public boolean accept(final NotificationEvent event) {
    for (IMetaStoreClient.NotificationFilter filter : filters) {
      if (!filter.accept(event)) {
        return false;
      }
    }
    return true;
  }
}
