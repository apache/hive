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
package org.apache.hadoop.hive.metastore;

/**
 * Keeps a list of reserved keys used by Hive listeners when updating the ListenerEvent
 * parameters.
 */
public class MetaStoreEventListenerConstants {
  /*
   * DbNotificationListener keys reserved for updating ListenerEvent parameters.
   *
   * DB_NOTIFICATION_EVENT_ID_KEY_NAME This key will have the event identifier that DbNotificationListener
   *                                   processed during an event. This event identifier might be shared
   *                                   across other MetaStoreEventListener implementations.
   */
  public static final String DB_NOTIFICATION_EVENT_ID_KEY_NAME = "DB_NOTIFICATION_EVENT_ID_KEY_NAME";

  /*
   * HiveMetaStore keys reserved for updating ListenerEvent parameters.
   *
   * HIVE_METASTORE_TRANSACTION_ACTIVE This key is used to check if a listener event is run inside a current
   *                                   transaction. A boolean value is used for active (true) or no active (false).
   */
  public static final String HIVE_METASTORE_TRANSACTION_ACTIVE = "HIVE_METASTORE_TRANSACTION_ACTIVE";
}