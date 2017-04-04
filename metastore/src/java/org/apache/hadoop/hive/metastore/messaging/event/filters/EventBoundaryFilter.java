package org.apache.hadoop.hive.metastore.messaging.event.filters;

import org.apache.hadoop.hive.metastore.api.NotificationEvent;

public class EventBoundaryFilter extends BasicFilter {
  private final long eventFrom, eventTo;

  public EventBoundaryFilter(final long eventFrom, final long eventTo) {
    this.eventFrom = eventFrom;
    this.eventTo = eventTo;
  }

  @Override
  boolean shouldAccept(final NotificationEvent event) {
    return eventFrom <= event.getEventId() && event.getEventId() <= eventTo;
  }
}
