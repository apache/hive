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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.events.AddCheckConstraintEvent;
import org.apache.hadoop.hive.metastore.events.AddDefaultConstraintEvent;
import org.apache.hadoop.hive.metastore.events.AddForeignKeyEvent;
import org.apache.hadoop.hive.metastore.events.AddNotNullConstraintEvent;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AddPrimaryKeyEvent;
import org.apache.hadoop.hive.metastore.events.AddSchemaVersionEvent;
import org.apache.hadoop.hive.metastore.events.AddUniqueConstraintEvent;
import org.apache.hadoop.hive.metastore.events.AlterCatalogEvent;
import org.apache.hadoop.hive.metastore.events.AlterDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.AlterDataConnectorEvent;
import org.apache.hadoop.hive.metastore.events.AlterISchemaEvent;
import org.apache.hadoop.hive.metastore.events.AlterPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterSchemaVersionEvent;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.events.BatchAcidWriteEvent;
import org.apache.hadoop.hive.metastore.events.CommitCompactionEvent;
import org.apache.hadoop.hive.metastore.events.CreateCatalogEvent;
import org.apache.hadoop.hive.metastore.events.CreateDataConnectorEvent;
import org.apache.hadoop.hive.metastore.events.CreateDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.CreateFunctionEvent;
import org.apache.hadoop.hive.metastore.events.CreateISchemaEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropCatalogEvent;
import org.apache.hadoop.hive.metastore.events.DropDataConnectorEvent;
import org.apache.hadoop.hive.metastore.events.DropDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.DropFunctionEvent;
import org.apache.hadoop.hive.metastore.events.DropISchemaEvent;
import org.apache.hadoop.hive.metastore.events.DropPartitionEvent;
import org.apache.hadoop.hive.metastore.events.DropSchemaVersionEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.apache.hadoop.hive.metastore.events.InsertEvent;
import org.apache.hadoop.hive.metastore.events.ListenerEvent;
import org.apache.hadoop.hive.metastore.events.OpenTxnEvent;
import org.apache.hadoop.hive.metastore.events.CommitTxnEvent;
import org.apache.hadoop.hive.metastore.events.AbortTxnEvent;
import org.apache.hadoop.hive.metastore.events.AllocWriteIdEvent;
import org.apache.hadoop.hive.metastore.events.AcidWriteEvent;
import org.apache.hadoop.hive.metastore.events.ReloadEvent;
import org.apache.hadoop.hive.metastore.events.UpdatePartitionColumnStatEventBatch;
import org.apache.hadoop.hive.metastore.events.UpdateTableColumnStatEvent;
import org.apache.hadoop.hive.metastore.events.DeleteTableColumnStatEvent;
import org.apache.hadoop.hive.metastore.events.UpdatePartitionColumnStatEvent;
import org.apache.hadoop.hive.metastore.events.DeletePartitionColumnStatEvent;
import org.apache.hadoop.hive.metastore.tools.SQLGenerator;
import java.sql.Connection;
import java.util.List;
import java.util.Map;
import static org.apache.hadoop.hive.metastore.MetaStoreEventListenerConstants.HIVE_METASTORE_TRANSACTION_ACTIVE;
import static org.apache.hadoop.hive.metastore.messaging.EventMessage.EventType;

/**
 * This class is used to notify a list of listeners about specific MetaStore events.
 */
@InterfaceAudience.Private
public class MetaStoreListenerNotifier {

  private interface EventNotifier {
    void notify(MetaStoreEventListener listener, ListenerEvent event) throws MetaException;
  }

  private static Map<EventType, EventNotifier> notificationEvents = Maps.newHashMap(
      ImmutableMap.<EventType, EventNotifier>builder()
          .put(EventType.CREATE_DATABASE, new EventNotifier() {
            @Override
            public void notify(MetaStoreEventListener listener,
                               ListenerEvent event) throws MetaException {
              listener.onCreateDatabase((CreateDatabaseEvent)event);
            }
          })
          .put(EventType.DROP_DATABASE, new EventNotifier() {
            @Override
            public void notify(MetaStoreEventListener listener, ListenerEvent event) throws MetaException {
              listener.onDropDatabase((DropDatabaseEvent)event);
            }
          })
          .put(EventType.CREATE_DATACONNECTOR, new EventNotifier() {
            @Override
            public void notify(MetaStoreEventListener listener,
                ListenerEvent event) throws MetaException {
              listener.onCreateDataConnector((CreateDataConnectorEvent)event);
            }
          })
          .put(EventType.DROP_DATACONNECTOR, new EventNotifier() {
            @Override
            public void notify(MetaStoreEventListener listener, ListenerEvent event) throws MetaException {
              listener.onDropDataConnector((DropDataConnectorEvent)event);
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
          .put(EventType.ALTER_DATABASE, new EventNotifier() {
            @Override
            public void notify(MetaStoreEventListener listener,
                               ListenerEvent event) throws MetaException {
              listener.onAlterDatabase((AlterDatabaseEvent)event);
            }
          })
          .put(EventType.ALTER_DATACONNECTOR, new EventNotifier() {
            @Override
            public void notify(MetaStoreEventListener listener,
                ListenerEvent event) throws MetaException {
              listener.onAlterDataConnector((AlterDataConnectorEvent)event);
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
          .put(EventType.ADD_PRIMARYKEY, new EventNotifier() {
            @Override
            public void notify(MetaStoreEventListener listener, ListenerEvent event) throws MetaException {
              listener.onAddPrimaryKey((AddPrimaryKeyEvent)event);
            }
          })
          .put(EventType.ADD_FOREIGNKEY, new EventNotifier() {
            @Override
            public void notify(MetaStoreEventListener listener, ListenerEvent event) throws MetaException {
              listener.onAddForeignKey((AddForeignKeyEvent)event);
            }
          })
          .put(EventType.ADD_UNIQUECONSTRAINT, new EventNotifier() {
            @Override
            public void notify(MetaStoreEventListener listener, ListenerEvent event) throws MetaException {
              listener.onAddUniqueConstraint((AddUniqueConstraintEvent)event);
            }
          })
          .put(EventType.ADD_NOTNULLCONSTRAINT, new EventNotifier() {
            @Override
            public void notify(MetaStoreEventListener listener, ListenerEvent event) throws MetaException {
              listener.onAddNotNullConstraint((AddNotNullConstraintEvent)event);
            }
          })
          .put(EventType.ADD_DEFAULTCONSTRAINT, new EventNotifier() {
            @Override
            public void notify(MetaStoreEventListener listener, ListenerEvent event) throws MetaException {
              listener.onAddDefaultConstraint((AddDefaultConstraintEvent) event);
            }
          })
          .put(EventType.ADD_CHECKCONSTRAINT, new EventNotifier() {
            @Override
            public void notify(MetaStoreEventListener listener, ListenerEvent event) throws MetaException {
              listener.onAddCheckConstraint((AddCheckConstraintEvent) event);
            }
          })
          .put(EventType.CREATE_ISCHEMA, new EventNotifier() {
            @Override
            public void notify(MetaStoreEventListener listener, ListenerEvent event) throws MetaException {
              listener.onCreateISchema((CreateISchemaEvent)event);
            }
          })
          .put(EventType.ALTER_ISCHEMA, new EventNotifier() {
            @Override
            public void notify(MetaStoreEventListener listener, ListenerEvent event) throws MetaException {
              listener.onAlterISchema((AlterISchemaEvent)event);
            }
          })
          .put(EventType.DROP_ISCHEMA, new EventNotifier() {
            @Override
            public void notify(MetaStoreEventListener listener, ListenerEvent event) throws MetaException {
              listener.onDropISchema((DropISchemaEvent)event);
            }
          })
          .put(EventType.ADD_SCHEMA_VERSION, new EventNotifier() {
            @Override
            public void notify(MetaStoreEventListener listener, ListenerEvent event) throws MetaException {
              listener.onAddSchemaVersion((AddSchemaVersionEvent) event);
            }
          })
          .put(EventType.ALTER_SCHEMA_VERSION, new EventNotifier() {
            @Override
            public void notify(MetaStoreEventListener listener, ListenerEvent event) throws MetaException {
              listener.onAlterSchemaVersion((AlterSchemaVersionEvent) event);
            }
          })
          .put(EventType.DROP_SCHEMA_VERSION, new EventNotifier() {
            @Override
            public void notify(MetaStoreEventListener listener, ListenerEvent event) throws MetaException {
              listener.onDropSchemaVersion((DropSchemaVersionEvent) event);
            }
          })
          .put(EventType.CREATE_CATALOG,
              (listener, event) -> listener.onCreateCatalog((CreateCatalogEvent)event))
          .put(EventType.DROP_CATALOG,
              (listener, event) -> listener.onDropCatalog((DropCatalogEvent)event))
          .put(EventType.ALTER_CATALOG,
              (listener, event) -> listener.onAlterCatalog((AlterCatalogEvent)event))
          .put(EventType.OPEN_TXN,
              (listener, event) -> listener.onOpenTxn((OpenTxnEvent) event, null, null))
          .put(EventType.COMMIT_TXN,
              (listener, event) -> listener.onCommitTxn((CommitTxnEvent) event, null, null))
          .put(EventType.ABORT_TXN,
              (listener, event) -> listener.onAbortTxn((AbortTxnEvent) event, null, null))
          .put(EventType.ALLOC_WRITE_ID,
              (listener, event) -> listener.onAllocWriteId((AllocWriteIdEvent) event, null, null))
          .put(EventType.ACID_WRITE,
                  (listener, event) -> listener.onAcidWrite((AcidWriteEvent) event, null, null))
          .put(EventType.BATCH_ACID_WRITE,
                  (listener, event) -> listener.onBatchAcidWrite((BatchAcidWriteEvent) event, null, null))
          .put(EventType.UPDATE_TABLE_COLUMN_STAT,
                      (listener, event) -> listener.onUpdateTableColumnStat((UpdateTableColumnStatEvent) event))
          .put(EventType.DELETE_TABLE_COLUMN_STAT,
                  (listener, event) -> listener.onDeleteTableColumnStat((DeleteTableColumnStatEvent) event))
          .put(EventType.UPDATE_PARTITION_COLUMN_STAT,
                  (listener, event) -> listener.onUpdatePartitionColumnStat((UpdatePartitionColumnStatEvent) event))
          .put(EventType.DELETE_PARTITION_COLUMN_STAT,
                  (listener, event) -> listener.onDeletePartitionColumnStat((DeletePartitionColumnStatEvent) event))
          .put(EventType.COMMIT_COMPACTION,
              ((listener, event) -> listener.onCommitCompaction((CommitCompactionEvent) event, null, null)))
          .put(EventType.RELOAD,
                  ((listener, event) -> listener.onReload((ReloadEvent) event)))
          .build()
  );


  private interface TxnEventNotifier {
    void notify(MetaStoreEventListener listener, ListenerEvent event, Connection dbConn, SQLGenerator sqlGenerator)
            throws MetaException;
  }

  private static Map<EventType, TxnEventNotifier> txnNotificationEvents = Maps.newHashMap(
    ImmutableMap.<EventType, TxnEventNotifier>builder()
      .put(EventType.OPEN_TXN,
        (listener, event, dbConn, sqlGenerator) -> listener.onOpenTxn((OpenTxnEvent) event, dbConn, sqlGenerator))
      .put(EventType.COMMIT_TXN,
        (listener, event, dbConn, sqlGenerator) -> listener.onCommitTxn((CommitTxnEvent) event, dbConn, sqlGenerator))
      .put(EventType.ABORT_TXN,
        (listener, event, dbConn, sqlGenerator) -> listener.onAbortTxn((AbortTxnEvent) event, dbConn, sqlGenerator))
      .put(EventType.ALLOC_WRITE_ID,
        (listener, event, dbConn, sqlGenerator) ->
                listener.onAllocWriteId((AllocWriteIdEvent) event, dbConn, sqlGenerator))
      .put(EventType.ACID_WRITE,
        (listener, event, dbConn, sqlGenerator) ->
                listener.onAcidWrite((AcidWriteEvent) event, dbConn, sqlGenerator))
      .put(EventType.BATCH_ACID_WRITE,
              (listener, event, dbConn, sqlGenerator) ->
                      listener.onBatchAcidWrite((BatchAcidWriteEvent) event, dbConn, sqlGenerator))
      .put(EventType.COMMIT_COMPACTION,
        (listener, event, dbConn, sqlGenerator) -> listener
            .onCommitCompaction((CommitCompactionEvent) event, dbConn, sqlGenerator))
      .put(EventType.UPDATE_PARTITION_COLUMN_STAT_BATCH,
              (listener, event, dbConn, sqlGenerator) ->
                      listener.onUpdatePartitionColumnStatInBatch((UpdatePartitionColumnStatEventBatch) event,
                            dbConn, sqlGenerator))
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
  public static Map<String, String> notifyEvent(List<? extends MetaStoreEventListener> listeners,
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
   * Notify a list of listeners about a specific metastore event to be executed within a txn. Each listener notified
   * might update the (ListenerEvent) event by setting a parameter key/value pair. These updated parameters will
   * be returned to the caller.
   *
   * @param listeners List of MetaStoreEventListener listeners.
   * @param eventType Type of the notification event.
   * @param event The ListenerEvent with information about the event.
   * @param dbConn The JDBC connection to the remote meta store db.
   * @param sqlGenerator The helper class to generate db specific SQL string.
   * @return A list of key/value pair parameters that the listeners set. The returned object will return an empty
   *         map if no parameters were updated or if no listeners were notified.
   * @throws MetaException If an error occurred while calling the listeners.
   */
  public static Map<String, String> notifyEventWithDirectSql(List<? extends MetaStoreEventListener> listeners,
                                                             EventType eventType,
                                                             ListenerEvent event,
                                                   Connection dbConn, SQLGenerator sqlGenerator) throws MetaException {

    Preconditions.checkNotNull(listeners, "Listeners must not be null.");
    Preconditions.checkNotNull(event, "The event must not be null.");

    for (MetaStoreEventListener listener : listeners) {
      txnNotificationEvents.get(eventType).notify(listener, event, dbConn, sqlGenerator);
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
  public static Map<String, String> notifyEvent(List<? extends MetaStoreEventListener> listeners,
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
   * Sometimes these events are run inside a DB transaction and might cause issues with the listeners,
   * for instance, Sentry blocks the HMS until an event is seen committed on the DB. To notify the listener about this,
   * a new parameter to verify if a transaction is active is added to the ListenerEvent, and is up to the listener
   * to skip this notification if so.
   *
   * @param listeners List of MetaStoreEventListener listeners.
   * @param eventType Type of the notification event.
   * @param event The ListenerEvent with information about the event.
   * @param environmentContext An EnvironmentContext object with parameters sent by the HMS client.
   * @param parameters A list of key/value pairs with the new parameters to add.
   * @param ms The RawStore object from where to check if a transaction is active.
   * @return A list of key/value pair parameters that the listeners set. The returned object will return an empty
   *         map if no parameters were updated or if no listeners were notified.
   * @throws MetaException If an error occurred while calling the listeners.
   */
  public static Map<String, String> notifyEvent(List<? extends MetaStoreEventListener> listeners,
                                                EventType eventType,
                                                ListenerEvent event,
                                                EnvironmentContext environmentContext,
                                                Map<String, String> parameters,
                                                final RawStore ms) throws MetaException {

    Preconditions.checkNotNull(event, "The event must not be null.");

    event.putParameters(parameters);

    if (ms != null) {
      event.putParameter(HIVE_METASTORE_TRANSACTION_ACTIVE, Boolean.toString(ms.isActiveTransaction()));
    }

    return notifyEvent(listeners, eventType, event, environmentContext);
  }
}
