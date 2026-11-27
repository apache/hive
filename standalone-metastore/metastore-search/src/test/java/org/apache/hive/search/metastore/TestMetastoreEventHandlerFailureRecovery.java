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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
import org.apache.hive.search.config.IndexOptions;
import org.apache.hive.search.exception.IndexNotHealthyException;
import org.apache.hive.search.search.InMemorySearchFixture;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
    conf.setInt(IndexOptions.EVENT_BATCH_MAX_FAILURES, 1);
    MessageBuilder messageBuilder = MessageBuilder.getInstance();
    Table orders = InMemorySearchFixture.table("hive", "sales", "orders", "sales orders");
    Table customers = InMemorySearchFixture.table("hive", "sales", "customers", "sales customers");
    NotificationEvent event1 = createEvent(101L, orders, messageBuilder);
    NotificationEvent event2 = createEvent(102L, customers, messageBuilder);
    IMetaStoreClient client = mock(IMetaStoreClient.class);
    when(client.getNextNotification(any(NotificationEventRequest.class), eq(true), eq(null)))
        .thenReturn(response(event1, event2))
        .thenReturn(emptyResponse());

    CountDownLatch applied = new CountDownLatch(2);
    AtomicInteger batchAttempts = new AtomicInteger();
    try (InMemorySearchFixture fixture = InMemorySearchFixture.create();
         MetastoreEventHandler handler = handler(conf, client)) {
      handler.addListeners(new MetastoreEventListener() {
        @Override
        public void notifyIndexTask(IndexTask task) throws IOException {
          if (task.tablesToAdd.size() > 1) {
            batchAttempts.incrementAndGet();
            throw new IOException("simulated batch apply failure");
          }
          apply(fixture, task);
          applied.countDown();
        }
      });

      handler.start(event1.getEventId() - 1, 0);
      await(applied);
      assertEquals(1, batchAttempts.get());
      assertFalse(fixture.searchMatch("orders", 5).isEmpty());
      assertFalse(fixture.searchMatch("customers", 5).isEmpty());
    }
  }

  @Test
  public void poisonEventMarksIndexUnhealthyAfterPartialApply() throws Exception {
    Configuration conf = eventHandlerConf();
    MessageBuilder messageBuilder = MessageBuilder.getInstance();
    NotificationEvent good = createEvent(201L,
        InMemorySearchFixture.table("hive", "sales", "orders", "sales orders"), messageBuilder);
    NotificationEvent poison = invalidCreate(202L);
    IMetaStoreClient client = mock(IMetaStoreClient.class);
    when(client.getNextNotification(any(NotificationEventRequest.class), eq(true), eq(null)))
        .thenReturn(response(good, poison))
        .thenReturn(emptyResponse());

    AtomicReference<IndexNotHealthyException> unhealthy = new AtomicReference<>();
    CountDownLatch halted = new CountDownLatch(1);
    try (InMemorySearchFixture fixture = InMemorySearchFixture.create();
         MetastoreEventHandler handler = handler(conf, client)) {
      handler.addListeners(new MetastoreEventListener() {
        @Override
        public void notifyIndexTask(IndexTask task) throws IOException {
          apply(fixture, task);
        }

        @Override
        public void notifyIndexState(boolean healthy, IndexNotHealthyException... errors) {
          if (!healthy && errors.length > 0) {
            unhealthy.set(errors[0]);
            halted.countDown();
          }
        }
      });

      handler.start(good.getEventId() - 1, 0);
      await(halted);
      assertTrue(unhealthy.get().getMessage().contains("202"));
      assertTrue(unhealthy.get().getMessage().contains("201"));
      assertFalse(fixture.searchMatch("orders", 5).isEmpty());
    }
  }

  @Test
  public void skipPoisonEventAdvancesCursorAndRecovers() throws Exception {
    Configuration conf = eventHandlerConf();
    conf.setBoolean(IndexOptions.EVENT_SKIP_POISON, true);
    MessageBuilder messageBuilder = MessageBuilder.getInstance();
    NotificationEvent good = createEvent(301L,
        InMemorySearchFixture.table("hive", "sales", "orders", "sales orders"), messageBuilder);
    NotificationEvent poison = invalidCreate(302L);

    CountDownLatch caughtUp = new CountDownLatch(1);
    IMetaStoreClient client = mock(IMetaStoreClient.class);
    when(client.getNextNotification(any(NotificationEventRequest.class), eq(true), eq(null)))
        .thenAnswer(invocation -> {
          NotificationEventRequest request = invocation.getArgument(0);
          if (request.getLastEvent() < poison.getEventId()) {
            return response(good, poison);
          }
          caughtUp.countDown();
          return emptyResponse();
        });

    try (InMemorySearchFixture fixture = InMemorySearchFixture.create();
         MetastoreEventHandler handler = handler(conf, client)) {
      handler.addListeners(new MetastoreEventListener() {
        @Override
        public void notifyIndexTask(IndexTask task) throws IOException {
          apply(fixture, task);
        }
      });

      handler.start(good.getEventId() - 1, 0);
      await(caughtUp);
      assertFalse(fixture.searchMatch("orders", 5).isEmpty());
    }
  }

  private static MetastoreEventHandler handler(Configuration conf, IMetaStoreClient client) {
    return new MetastoreEventHandler(conf,
        configuration -> new MetastoreClientProvider.Client(client, false));
  }

  private static void apply(InMemorySearchFixture fixture, MetastoreEventListener.IndexTask task)
      throws IOException {
    try {
      fixture.mutations().apply(task);
      fixture.commit(task.lastEventId);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  private static void await(CountDownLatch latch) throws InterruptedException {
    assertTrue(latch.await(5, TimeUnit.SECONDS));
  }

  private static Configuration eventHandlerConf() {
    Configuration conf = new Configuration(false);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.EVENT_MESSAGE_FACTORY,
        JSONMessageEncoder.class.getName());
    conf.setLong(IndexOptions.EVENT_FAILURE_BACKOFF_MS, 0L);
    conf.setInt(IndexOptions.EVENT_BATCH_MAX_FAILURES, 3);
    return conf;
  }

  private static NotificationEvent createEvent(long id, Table table, MessageBuilder builder) {
    return event(id, MessageBuilder.CREATE_TABLE_EVENT,
        builder.buildCreateTableMessage(table, null).toString(),
        table.getCatName(), table.getDbName(), table.getTableName());
  }

  private static NotificationEvent invalidCreate(long id) {
    return event(id, MessageBuilder.CREATE_TABLE_EVENT, "{invalid-create-table-message",
        "hive", "sales", "broken");
  }

  private static NotificationEvent event(
      long id, String type, String message, String cat, String db, String table) {
    NotificationEvent event = new NotificationEvent(id, 0, type, message);
    event.setMessageFormat(JSONMessageEncoder.FORMAT);
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
