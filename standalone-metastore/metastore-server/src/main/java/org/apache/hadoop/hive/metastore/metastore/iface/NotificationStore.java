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

package org.apache.hadoop.hive.metastore.metastore.iface;

import java.util.List;

import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.NotificationEventRequest;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.api.NotificationEventsCountRequest;
import org.apache.hadoop.hive.metastore.api.NotificationEventsCountResponse;
import org.apache.hadoop.hive.metastore.api.WriteEventInfo;
import org.apache.hadoop.hive.metastore.metastore.MetaDescriptor;
import org.apache.hadoop.hive.metastore.metastore.impl.NotificationStoreImpl;

@MetaDescriptor(alias = "notification", defaultImpl = NotificationStoreImpl.class)
public interface NotificationStore {
  /**
   * Get the next notification event.
   * @param rqst Request containing information on the last processed notification.
   * @return list of notifications, sorted by eventId
   */
  NotificationEventResponse getNextNotification(NotificationEventRequest rqst);


  /**
   * Add a notification entry.  This should only be called from inside the metastore
   * @param event the notification to add
   * @throws MetaException error accessing RDBMS
   */
  void addNotificationEvent(NotificationEvent event) throws MetaException;

  /**
   * Remove older notification events.
   *
   * @param olderThan Remove any events older or equal to a given number of seconds
   */
  void cleanNotificationEvents(int olderThan);

  /**
   * Get the last issued notification event id.  This is intended for use by the export command
   * so that users can determine the state of the system at the point of the export,
   * and determine which notification events happened before or after the export.
   * @return
   */
  CurrentNotificationEventId getCurrentNotificationEventId();

  /**
   * Get the number of events corresponding to given database with fromEventId.
   * This is intended for use by the repl commands to track the progress of incremental dump.
   * @return
   */
  NotificationEventsCountResponse getNotificationEventsCount(NotificationEventsCountRequest rqst);

  /**
   * Remove older notification events.
   * @param olderThan Remove any events older or equal to a given number of seconds
   */
  void cleanWriteNotificationEvents(int olderThan);

  /**
   * Get all write events for a specific transaction .
   * @param txnId get all the events done by this transaction
   * @param dbName the name of db for which dump is being taken
   * @param tableName the name of the table for which the dump is being taken
   */
  List<WriteEventInfo> getAllWriteEventInfo(long txnId, String dbName, String tableName) throws MetaException;
}
