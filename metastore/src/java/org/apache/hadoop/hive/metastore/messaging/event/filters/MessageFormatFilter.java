package org.apache.hadoop.hive.metastore.messaging.event.filters;

import org.apache.hadoop.hive.metastore.api.NotificationEvent;

public class MessageFormatFilter extends BasicFilter {
  private final String format;

  public MessageFormatFilter(String format) {
    this.format = format;
  }

  @Override
  boolean shouldAccept(final NotificationEvent event) {
    if (format == null) {
      return true; // let's say that passing null in will not do any filtering.
    }
    return format.equalsIgnoreCase(event.getMessageFormat());
  }
}
