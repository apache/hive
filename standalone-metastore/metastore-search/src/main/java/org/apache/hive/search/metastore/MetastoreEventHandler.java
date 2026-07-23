/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.search.metastore;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.DatabaseName;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.NotificationEventRequest;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.messaging.AlterTableMessage;
import org.apache.hadoop.hive.metastore.messaging.CreateTableMessage;
import org.apache.hadoop.hive.metastore.messaging.MessageBuilder;
import org.apache.hadoop.hive.metastore.messaging.MessageDeserializer;
import org.apache.hadoop.hive.metastore.messaging.MessageFactory;
import org.apache.hive.search.exception.IndexNotHealthyException;
import org.apache.hive.search.config.IndexOptions;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Polls HMS notification events, coalesces them into batches, and dispatches index mutations.
 *
 * <p>On repeated batch failure the handler falls back to single-event apply so one poison event
 * does not block the entire batch forever. If a poison event still cannot be applied, the index
 * is marked unhealthy and search is blocked until the root cause is fixed.
 */
public class MetastoreEventHandler implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(MetastoreEventHandler.class);
  private final AtomicBoolean stopped = new AtomicBoolean(false);
  private final List<MetastoreEventListener> listeners = Collections.synchronizedList(new ArrayList<>());
  private final IndexOptions indexConfig;
  private final IMetaStoreClient client;
  private final MessageDeserializer deserializer;
  private Thread metaRefresher;
  private long lastEventId;

  /** Consecutive batch failures at {@link #failedBatchStartId}. */
  private int consecutiveBatchFailures;
  private long failedBatchStartId = -1;

  private MetastoreEventHandler(Configuration configuration) throws TException {
    this(configuration, RetryingMetaStoreClient.getProxy(configuration, true));
  }

  /** Package-private for unit tests with a stub {@link IMetaStoreClient}. */
  MetastoreEventHandler(Configuration configuration, IMetaStoreClient client) {
    Configuration conf = new Configuration(Objects.requireNonNull(configuration));
    this.indexConfig = new IndexOptions(conf);
    this.client = Objects.requireNonNull(client);
    this.deserializer = MessageFactory.getDefaultInstance(conf).getDeserializer();
  }

  public static MetastoreEventHandler of(Configuration conf, MetastoreEventListener... listeners)
      throws TException {
    MetastoreEventHandler catalogService = new MetastoreEventHandler(conf);
    return catalogService.addListeners(listeners);
  }

  public MetastoreEventHandler addListeners(MetastoreEventListener... listeners) {
    return addListeners(Arrays.asList(listeners));
  }

  public MetastoreEventHandler addListeners(Collection<MetastoreEventListener> listeners) {
    this.listeners.addAll(listeners);
    return this;
  }

  public MetastoreEventHandler start(long nid) throws Exception {
    this.lastEventId = nid;
    final long sleepInterval = indexConfig.getPollNotificationInterval();
    final int pollMax = indexConfig.getEventPollMax();
    final long unhealthyBackoffMs = indexConfig.getEventUnhealthyBackoffMs();
    this.metaRefresher = new Thread(() -> {
      while (!stopped.get()) {
        try {
          int processed = getNextMetastoreEvents(pollMax);
          if (processed <= 0) {
            Thread.sleep(sleepInterval);
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        } catch (IndexNotHealthyException e) {
          LOG.error("Incremental index update halted: {}", e.getMessage(), e);
          notifyListenersStatus(false, e);
          try {
            Thread.sleep(unhealthyBackoffMs);
          } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            break;
          }
        }
      }
    });
    this.metaRefresher.setName("Metastore-Event-Poller");
    this.metaRefresher.start();
    return this;
  }

  @Override
  public void close() throws Exception {
    stopped.set(true);
    if (metaRefresher != null) {
      metaRefresher.interrupt();
      metaRefresher.join();
    }
    if (client != null) {
      client.close();
    }
  }

  public int getNextMetastoreEvents(int eventCount) throws IndexNotHealthyException {
    try {
      NotificationEventRequest request = new NotificationEventRequest(lastEventId);
      request.setMaxEvents(eventCount);
      NotificationEventResponse resp = client.getNextNotification(request, true, null);
      if (resp == null || resp.getEvents() == null || resp.getEvents().isEmpty()) {
        LOG.debug("No event found since the last event id: {}", lastEventId);
        return 0;
      }
      List<NotificationEvent> events = resp.getEvents();
      long batchStartId = events.get(0).getEventId();

      MetastoreEventListener.IndexTask task;
      try {
        task = buildTask(events);
      } catch (Exception parseError) {
        LOG.warn(
            "Failed to build notification batch starting at event {}; falling back to single-event apply",
            batchStartId,
            parseError);
        int applied = applyEventsIndividually(events);
        if (applied > 0) {
          notifyListenersStatus(true);
          resetBatchFailureState();
          return applied;
        }
        backoffAfterFailure();
        return 0;
      }
      try {
        notifyListeners(task);
        lastEventId = task.lastEventId;
        notifyListenersStatus(true);
        resetBatchFailureState();
        return events.size();
      } catch (Exception batchError) {
        recordBatchFailure(batchStartId, task, batchError);
        if (consecutiveBatchFailures >= indexConfig.getEventBatchMaxFailures()) {
          LOG.warn(
              "Batch apply failed {} time(s) at event {}; falling back to single-event apply",
              consecutiveBatchFailures,
              failedBatchStartId);
          int applied = applyEventsIndividually(events);
          if (applied > 0) {
            notifyListenersStatus(true);
            resetBatchFailureState();
            return applied;
          }
        }
        backoffAfterFailure();
        return 0;
      }
    } catch (IndexNotHealthyException e) {
      throw e;
    } catch (Exception e) {
      LOG.warn("Failed to fetch or parse notification events after lastEventId={}", lastEventId, e);
      backoffAfterFailure();
    }
    return 0;
  }

  private int applyEventsIndividually(List<NotificationEvent> events)
      throws IndexNotHealthyException {
    int applied = 0;
    for (NotificationEvent event : events) {
      if (lastEventId >= event.getEventId()) {
        continue;
      }
      MetastoreEventListener.IndexTask task =
          new MetastoreEventListener.IndexTask();
      task.firstEventId = event.getEventId();
      try {
        dispatchEvent(event, task);
        task.lastEventId = event.getEventId();
        notifyListeners(task);
        lastEventId = event.getEventId();
        applied++;
      } catch (Exception e) {
        long poisonEventId = event.getEventId();
        String poisonEventType = event.getEventType();
        if (indexConfig.isEventSkipPoison()) {
          LOG.error(
              "Skipping poison notification event {} (type={}); index may be stale for this object",
              poisonEventId,
              poisonEventType,
              e);
          lastEventId = event.getEventId();
          applied++;
        } else {
          LOG.error(
              "Stuck on notification event {} (type={}); committed lastEventId={}",
              poisonEventId,
              poisonEventType,
              lastEventId,
              e);
          String progress = applied > 0
              ? applied + " prior event(s) in batch were applied; "
              : "";
          throw new IndexNotHealthyException(
              "Cannot apply notification event "
                  + poisonEventId
                  + " (type="
                  + poisonEventType
                  + "); "
                  + progress
                  + "committed lastEventId="
                  + lastEventId
                  + ". Fix root cause, set "
                  + IndexOptions.EVENT_SKIP_POISON
                  + "=true to skip, or rebuild the index.",
              e);
        }
      }
    }
    if (applied > 0) {
      LOG.info("Applied {} notification event(s) individually; lastEventId={}", applied, lastEventId);
    }
    return applied;
  }

  private MetastoreEventListener.IndexTask buildTask(List<NotificationEvent> events)
      throws Exception {
    MetastoreEventListener.IndexTask task = new MetastoreEventListener.IndexTask();
    for (int i = 0; i < events.size(); i++) {
      NotificationEvent event = events.get(i);
      if (lastEventId >= event.getEventId()) {
        throw new IllegalStateException(
            "Out-of-order metastore notification event: lastEventId=" + lastEventId
                + ", eventId=" + event.getEventId());
      }
      if (i == 0) {
        task.firstEventId = event.getEventId();
      }
      dispatchEvent(event, task);
      task.lastEventId = event.getEventId();
    }
    return task;
  }

  private void notifyListeners(MetastoreEventListener.IndexTask task) throws Exception {
    for (MetastoreEventListener listener : listeners) {
      listener.notifyIndexTask(task);
    }
  }

  private void notifyListenersStatus(boolean healthy, IndexNotHealthyException... errors) {
    for (MetastoreEventListener listener : listeners) {
      listener.notifyIndexState(healthy, errors);
    }
  }

  private void recordBatchFailure(
      long batchStartId, MetastoreEventListener.IndexTask task, Exception e) {
    if (failedBatchStartId != batchStartId) {
      failedBatchStartId = batchStartId;
      consecutiveBatchFailures = 1;
    } else {
      consecutiveBatchFailures++;
    }
    LOG.warn(
        "Failed to apply notification batch (attempt {}/{}, committed lastEventId={}, "
            + "batch event range {}-{}, upserts={}, tableDrops={}, dbDrops={})",
        consecutiveBatchFailures,
        indexConfig.getEventBatchMaxFailures(),
        lastEventId,
        task.firstEventId,
        task.lastEventId,
        task.tablesToAdd.size(),
        task.tablesToDrop.size(),
        task.databasesToDrop.size(),
        e);
  }

  private void resetBatchFailureState() {
    consecutiveBatchFailures = 0;
    failedBatchStartId = -1;
  }

  private void backoffAfterFailure() {
    long backoffMs = indexConfig.getEventFailureBackoffMs();
    if (backoffMs <= 0) {
      return;
    }
    try {
      Thread.sleep(backoffMs);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private void dispatchEvent(
      NotificationEvent event,
      MetastoreEventListener.IndexTask task)
      throws Exception {
    String message = event.getMessage();
    switch (event.getEventType()) {
      case MessageBuilder.CREATE_TABLE_EVENT -> {
        CreateTableMessage createTableMessage = deserializer.getCreateTableMessage(message);
        Table table = createTableMessage.getTableObj();
        TableName tableName = new TableName(table.getCatName(), table.getDbName(), table.getTableName());
        task.tablesToAdd.put(tableName, table);
      }
      case MessageBuilder.DROP_TABLE_EVENT -> {
        deserializer.getDropTableMessage(message);
        TableName tableName = new TableName(event.getCatName(), event.getDbName(), event.getTableName());
        if (task.tablesToAdd.containsKey(tableName)) {
          task.tablesToAdd.remove(tableName);
        } else {
          task.tablesToDrop.add(tableName);
        }
      }
      case MessageBuilder.DROP_DATABASE_EVENT -> {
        DatabaseName databaseName = new DatabaseName(event.getCatName(), event.getDbName());
        task.databasesToDrop.add(databaseName);
        for (TableName tableName : new ArrayList<>(task.tablesToAdd.keySet())) {
          if (databaseName.equals(new DatabaseName(tableName.getCat(), tableName.getDb()))) {
            task.tablesToAdd.remove(tableName);
          }
        }
        task.tablesToDrop.removeIf(
            tableName -> databaseName.equals(new DatabaseName(tableName.getCat(), tableName.getDb())));
      }
      case MessageBuilder.ALTER_TABLE_EVENT -> {
        AlterTableMessage alterTableMessage = deserializer.getAlterTableMessage(message);
        Table tableAfter = alterTableMessage.getTableObjAfter();
        Table tableBefore = alterTableMessage.getTableObjBefore();
        if (!MetastoreTableMapper.hasIndexedFieldsChanged(tableBefore, tableAfter)) {
          break;
        }
        TableName tblNameBefore = new TableName(tableBefore.getCatName(),
            tableBefore.getDbName(), tableBefore.getTableName());
        TableName tblNameAfter = new TableName(tableAfter.getCatName(),
            tableAfter.getDbName(), tableAfter.getTableName());
        if (task.tablesToAdd.containsKey(tblNameBefore)) {
          task.tablesToAdd.remove(tblNameBefore);
        } else {
          task.tablesToDrop.add(tblNameBefore);
        }
        task.tablesToAdd.put(tblNameAfter, tableAfter);
      }
      default -> {
        // Ignored event types still advance the notification cursor when applied individually.
      }
    }
  }
}
