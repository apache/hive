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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.messaging.json.JSONMessageEncoder;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.NotificationEventRequest;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.messaging.MessageBuilder;
import org.apache.hive.search.config.IndexConfig;
import org.apache.hive.search.exception.IndexNotHealthyException;
import org.apache.hive.search.search.InMemorySearchFixture;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Category(MetastoreUnitTest.class)
public class TestMetastoreEventHandlerFailureRecovery {

  @Test
  public void batchFailureFallsBackToSingleEventApply() throws Exception {
    Configuration conf = eventHandlerConf();
    conf.setInt(IndexConfig.EVENT_BATCH_MAX_FAILURES, 1);

    MessageBuilder messageBuilder = MessageBuilder.getInstance();
    Table orders = InMemorySearchFixture.table("hive", "sales", "orders", "sales orders");
    Table customers = InMemorySearchFixture.table("hive", "sales", "customers", "sales customers");
    NotificationEvent event1 = createEvent(101L, orders, messageBuilder);
    NotificationEvent event2 = createEvent(102L, customers, messageBuilder);

    IMetaStoreClient client = mock(IMetaStoreClient.class);
    when(client.getNextNotification(any(NotificationEventRequest.class), eq(true), eq(null)))
        .thenReturn(response(event1, event2))
        .thenReturn(emptyResponse());

    try (InMemorySearchFixture fixture = InMemorySearchFixture.create()) {
      AtomicInteger batchAttempts = new AtomicInteger();
      MetastoreEventHandler handler = new MetastoreEventHandler(conf, client);
      handler.addListeners(new MetastoreEventListener() {
        @Override
        public void notifyIndexTask(IndexTask task) throws IOException {
          if (task.tablesToAdd.size() > 1) {
            batchAttempts.incrementAndGet();
            throw new IOException("simulated batch apply failure");
          }
          try {
            fixture.mutations().apply(task);
            fixture.commit(task.lastEventId);
          } catch (Exception e) {
            throw new IOException(e);
          }
        }
      });

      assertEquals(2, handler.getNextMetastoreEvents(10));
      assertEquals(1, batchAttempts.get());
      assertFalse(fixture.searchMatch("orders", 5).isEmpty());
      assertFalse(fixture.searchMatch("customers", 5).isEmpty());
    }
  }

  @Test
  public void poisonEventMarksIndexUnhealthyAfterPartialApply() throws Exception {
    Configuration conf = eventHandlerConf();
    MessageBuilder messageBuilder = MessageBuilder.getInstance();
    Table orders = InMemorySearchFixture.table("hive", "sales", "orders", "sales orders");
    NotificationEvent good = createEvent(201L, orders, messageBuilder);
    NotificationEvent poison = event(
        202L,
        MessageBuilder.CREATE_TABLE_EVENT,
        "{invalid-create-table-message",
        "hive",
        "sales",
        "broken");

    IMetaStoreClient client = mock(IMetaStoreClient.class);
    when(client.getNextNotification(any(NotificationEventRequest.class), eq(true), eq(null)))
        .thenReturn(response(good, poison));

    try (InMemorySearchFixture fixture = InMemorySearchFixture.create()) {
      MetastoreEventHandler handler = new MetastoreEventHandler(conf, client);
      handler.addListeners(new MetastoreEventListener() {
        @Override
        public void notifyIndexTask(IndexTask task) throws IOException {
          try {
            fixture.mutations().apply(task);
            fixture.commit(task.lastEventId);
          } catch (Exception e) {
            throw new IOException(e);
          }
        }
      });

      IndexNotHealthyException error =
          assertThrows(IndexNotHealthyException.class, () -> handler.getNextMetastoreEvents(10));
      assertTrue(error.getMessage().contains("202"));
      assertTrue(error.getMessage().contains("201"));
      assertFalse(fixture.searchMatch("orders", 5).isEmpty());
    }
  }

  @Test
  public void skipPoisonEventAdvancesCursorAndRecovers() throws Exception {
    Configuration conf = eventHandlerConf();
    conf.setBoolean(IndexConfig.EVENT_SKIP_POISON, true);

    MessageBuilder messageBuilder = MessageBuilder.getInstance();
    Table orders = InMemorySearchFixture.table("hive", "sales", "orders", "sales orders");
    NotificationEvent good = createEvent(301L, orders, messageBuilder);
    NotificationEvent poison = event(
        302L,
        MessageBuilder.CREATE_TABLE_EVENT,
        "{invalid-create-table-message",
        "hive",
        "sales",
        "broken");

    IMetaStoreClient client = mock(IMetaStoreClient.class);
    when(client.getNextNotification(any(NotificationEventRequest.class), eq(true), eq(null)))
        .thenReturn(response(good, poison))
        .thenReturn(emptyResponse());

    try (InMemorySearchFixture fixture = InMemorySearchFixture.create()) {
      MetastoreEventHandler handler = new MetastoreEventHandler(conf, client);
      handler.addListeners(new MetastoreEventListener() {
        @Override
        public void notifyIndexTask(IndexTask task) throws IOException {
          try {
            fixture.mutations().apply(task);
            fixture.commit(task.lastEventId);
          } catch (Exception e) {
            throw new IOException(e);
          }
        }
      });

      assertEquals(2, handler.getNextMetastoreEvents(10));
      assertFalse(fixture.searchMatch("orders", 5).isEmpty());
      assertEquals(0, handler.getNextMetastoreEvents(10));
    }
  }

  @Test
  public void parseFailureFallsBackToSingleEventApply() throws Exception {
    Configuration conf = eventHandlerConf();
    MessageBuilder messageBuilder = MessageBuilder.getInstance();
    Table orders = InMemorySearchFixture.table("hive", "sales", "orders", "sales orders");
    NotificationEvent good = createEvent(401L, orders, messageBuilder);
    NotificationEvent unparsableBatchMember = event(
        402L,
        MessageBuilder.CREATE_TABLE_EVENT,
        "{invalid-create-table-message",
        "hive",
        "sales",
        "broken");

    IMetaStoreClient client = mock(IMetaStoreClient.class);
    when(client.getNextNotification(any(NotificationEventRequest.class), eq(true), eq(null)))
        .thenReturn(response(good, unparsableBatchMember));

    try (InMemorySearchFixture fixture = InMemorySearchFixture.create()) {
      AtomicBoolean sawBatchFailure = new AtomicBoolean();
      MetastoreEventHandler handler = new MetastoreEventHandler(conf, client);
      handler.addListeners(new MetastoreEventListener() {
        @Override
        public void notifyIndexTask(IndexTask task) throws IOException {
          if (task.tablesToAdd.size() > 1) {
            sawBatchFailure.set(true);
          }
          try {
            fixture.mutations().apply(task);
            fixture.commit(task.lastEventId);
          } catch (Exception e) {
            throw new IOException(e);
          }
        }
      });

      assertThrows(IndexNotHealthyException.class, () -> handler.getNextMetastoreEvents(10));
      assertFalse(sawBatchFailure.get());
      assertFalse(fixture.searchMatch("orders", 5).isEmpty());
    }
  }

  private static Configuration eventHandlerConf() {
    Configuration conf = new Configuration(false);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.EVENT_MESSAGE_FACTORY,
        JSONMessageEncoder.class.getName());
    conf.setLong(IndexConfig.EVENT_FAILURE_BACKOFF_MS, 0L);
    conf.setInt(IndexConfig.EVENT_BATCH_MAX_FAILURES, 3);
    return conf;
  }

  private static NotificationEvent createEvent(long id, Table table, MessageBuilder builder) {
    return event(
        id,
        MessageBuilder.CREATE_TABLE_EVENT,
        builder.buildCreateTableMessage(table, null).toString(),
        table.getCatName(),
        table.getDbName(),
        table.getTableName());
  }

  private static NotificationEvent event(
      long id, String type, String message, String cat, String db, String table) {
    NotificationEvent event = new NotificationEvent(id, 0, type, message);
    event.setCatName(cat);
    event.setDbName(db);
    event.setTableName(table);
    return event;
  }

  private static NotificationEventResponse response(NotificationEvent... events) {
    NotificationEventResponse response = new NotificationEventResponse();
    response.setEvents(List.of(events));
    return response;
  }

  private static NotificationEventResponse emptyResponse() {
    NotificationEventResponse response = new NotificationEventResponse();
    response.setEvents(new ArrayList<>());
    return response;
  }
}
