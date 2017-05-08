/**
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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.hadoop.hive.common.classification.InterfaceAudience.Private;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.events.AddIndexEvent;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterIndexEvent;
import org.apache.hadoop.hive.metastore.events.AlterPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.events.CreateDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.CreateFunctionEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.DropFunctionEvent;
import org.apache.hadoop.hive.metastore.events.DropIndexEvent;
import org.apache.hadoop.hive.metastore.events.DropPartitionEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.apache.hadoop.hive.metastore.events.InsertEvent;
import org.apache.hadoop.hive.metastore.events.ListenerEvent;

import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hive.metastore.messaging.EventMessage.EventType;

/**
 * This class is used to notify a list of listeners about specific MetaStore events.
 */
@Private
public class MetaStoreListenerNotifier {
  private interface EventNotifier {
    void notify(MetaStoreEventListener listener, ListenerEvent event) throws MetaException;
  }

  private static Map<EventType, EventNotifier> notificationEvents = Maps.newHashMap(
      ImmutableMap.<EventType, EventNotifier>builder()
          .put(EventType.CREATE_DATABASE, new EventNotifier() {
            @Override
            public void notify(MetaStoreEventListener listener, ListenerEvent event) throws MetaException {
              listener.onCreateDatabase((CreateDatabaseEvent)event);
            }
          })
          .put(EventType.DROP_DATABASE, new EventNotifier() {
            @Override
            public void notify(MetaStoreEventListener listener, ListenerEvent event) throws MetaException {
              listener.onDropDatabase((DropDatabaseEvent)event);
            }
          })
          .put(EventType.CREATE_TABLE, new EventNotifier() {
            @Override
            public void notify(MetaStoreEventListener listener, ListenerEvent event) throws MetaException {
              listener.onCreateTable((CreateTableEvent)event);
            }
          })
          .put(EventType.DROP_TABLE, new EventNotifier() {
            @Override
            public void notify(MetaStoreEventListener listener, ListenerEvent event) throws MetaException {
              listener.onDropTable((DropTableEvent)event);
            }
          })
          .put(EventType.ADD_PARTITION, new EventNotifier() {
            @Override
            public void notify(MetaStoreEventListener listener, ListenerEvent event) throws MetaException {
              listener.onAddPartition((AddPartitionEvent)event);
            }
          })
          .put(EventType.DROP_PARTITION, new EventNotifier() {
            @Override
            public void notify(MetaStoreEventListener listener, ListenerEvent event) throws MetaException {
              listener.onDropPartition((DropPartitionEvent)event);
            }
          })
          .put(EventType.ALTER_TABLE, new EventNotifier() {
            @Override
            public void notify(MetaStoreEventListener listener, ListenerEvent event) throws MetaException {
              listener.onAlterTable((AlterTableEvent)event);
            }
          })
          .put(EventType.ALTER_PARTITION, new EventNotifier() {
            @Override
            public void notify(MetaStoreEventListener listener, ListenerEvent event) throws MetaException {
              listener.onAlterPartition((AlterPartitionEvent)event);
            }
          })
          .put(EventType.INSERT, new EventNotifier() {
            @Override
            public void notify(MetaStoreEventListener listener, ListenerEvent event) throws MetaException {
              listener.onInsert((InsertEvent)event);
            }
          })
          .put(EventType.CREATE_FUNCTION, new EventNotifier() {
            @Override
            public void notify(MetaStoreEventListener listener, ListenerEvent event) throws MetaException {
              listener.onCreateFunction((CreateFunctionEvent)event);
            }
          })
          .put(EventType.DROP_FUNCTION, new EventNotifier() {
            @Override
            public void notify(MetaStoreEventListener listener, ListenerEvent event) throws MetaException {
              listener.onDropFunction((DropFunctionEvent)event);
            }
          })
          .put(EventType.CREATE_INDEX, new EventNotifier() {
            @Override
            public void notify(MetaStoreEventListener listener, ListenerEvent event) throws MetaException {
              listener.onAddIndex((AddIndexEvent)event);
            }
          })
          .put(EventType.DROP_INDEX, new EventNotifier() {
            @Override
            public void notify(MetaStoreEventListener listener, ListenerEvent event) throws MetaException {
              listener.onDropIndex((DropIndexEvent)event);
            }
          })
          .put(EventType.ALTER_INDEX, new EventNotifier() {
            @Override
            public void notify(MetaStoreEventListener listener, ListenerEvent event) throws MetaException {
              listener.onAlterIndex((AlterIndexEvent)event);
            }
          })
          .build()
  );

  /**
   * Notify a list of listeners about a specific metastore event. Each listener notified might update
   * the (ListenerEvent) event by setting a parameter key/value pair. These updated parameters will
   * be returned to the caller.
   *
   * @param listeners List of MetaStoreEventListener listeners.
   * @param eventType Type of the notification event.
   * @param event The ListenerEvent with information about the event.
   * @return A list of key/value pair parameters that the listeners set. The returned object will return an empty
   *         map if no parameters were updated or if no listeners were notified.
   * @throws MetaException If an error occurred while calling the listeners.
   */
  public static Map<String, String> notifyEvent(List<MetaStoreEventListener> listeners,
                                                EventType eventType,
                                                ListenerEvent event) throws MetaException {

    Preconditions.checkNotNull(listeners, "Listeners must not be null.");
    Preconditions.checkNotNull(event, "The event must not be null.");

    for (MetaStoreEventListener listener : listeners) {
      notificationEvents.get(eventType).notify(listener, event);
    }

    // Each listener called above might set a different parameter on the event.
    // This write permission is allowed on the listener side to avoid breaking compatibility if we change the API
    // method calls.
    return event.getParameters();
  }

  /**
   * Notify a list of listeners about a specific metastore event. Each listener notified might update
   * the (ListenerEvent) event by setting a parameter key/value pair. These updated parameters will
   * be returned to the caller.
   *
   * @param listeners List of MetaStoreEventListener listeners.
   * @param eventType Type of the notification event.
   * @param event The ListenerEvent with information about the event.
   * @param environmentContext An EnvironmentContext object with parameters sent by the HMS client.
   * @return A list of key/value pair parameters that the listeners set. The returned object will return an empty
   *         map if no parameters were updated or if no listeners were notified.
   * @throws MetaException If an error occurred while calling the listeners.
   */
  public static Map<String, String> notifyEvent(List<MetaStoreEventListener> listeners,
                                                EventType eventType,
                                                ListenerEvent event,
                                                EnvironmentContext environmentContext) throws MetaException {

    Preconditions.checkNotNull(event, "The event must not be null.");

    event.setEnvironmentContext(environmentContext);
    return notifyEvent(listeners, eventType, event);
  }

  /**
   * Notify a list of listeners about a specific metastore event. Each listener notified might update
   * the (ListenerEvent) event by setting a parameter key/value pair. These updated parameters will
   * be returned to the caller.
   *
   * @param listeners List of MetaStoreEventListener listeners.
   * @param eventType Type of the notification event.
   * @param event The ListenerEvent with information about the event.
   * @param environmentContext An EnvironmentContext object with parameters sent by the HMS client.
   * @param parameters A list of key/value pairs with the new parameters to add.
   * @return A list of key/value pair parameters that the listeners set. The returned object will return an empty
   *         map if no parameters were updated or if no listeners were notified.
   * @throws MetaException If an error occurred while calling the listeners.
   */
  public static Map<String, String> notifyEvent(List<MetaStoreEventListener> listeners,
                                                EventType eventType,
                                                ListenerEvent event,
                                                EnvironmentContext environmentContext,
                                                Map<String, String> parameters) throws MetaException {

    Preconditions.checkNotNull(event, "The event must not be null.");

    event.putParameters(parameters);
    return notifyEvent(listeners, eventType, event, environmentContext);
  }
}
