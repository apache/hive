/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore.messaging.event.filters;

import org.apache.hadoop.hive.common.repl.ReplScope;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.messaging.MessageBuilder;

/**
 * Utility function that constructs a notification filter to check if table is accepted for replication.
 */
public class ReplEventFilter extends BasicFilter {
  private final ReplScope replScope;

  public ReplEventFilter(final ReplScope replScope) {
    this.replScope = replScope;
  }

  @Override
  boolean shouldAccept(final NotificationEvent event) {
    // All txn related events are global ones and should be always accepted.
    // For other events, if the DB/table names are included as per replication scope, then should
    // accept the event. For alter table with table name filter, bootstrap of the table will be done if the new table
    // name matches the filter but the old table name does not. This can be judge only after deserialize of the message.
    return (isTxnRelatedEvent(event)
        || replScope.includedInReplScope(event.getDbName(), event.getTableName())
        || (replScope.dbIncludedInReplScope(event.getDbName())
                    && event.getEventType().equals(MessageBuilder.ALTER_TABLE_EVENT))
    );
  }
}
