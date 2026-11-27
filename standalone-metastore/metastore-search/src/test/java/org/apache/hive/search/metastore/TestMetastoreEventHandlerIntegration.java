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
import org.apache.hive.search.search.InMemorySearchFixture;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Category(MetastoreUnitTest.class)
public class TestMetastoreEventHandlerIntegration {

  @Test
  public void refreshesTableOnAlterWhenIndexedFieldsUnchanged() throws Exception {
    Configuration conf = eventHandlerConf();
    MessageBuilder messageBuilder = MessageBuilder.getInstance();
    Table before = InMemorySearchFixture.table("hive", "sales", "orders", "daily sales orders");
    Table after = new Table(before);
    after.setParameters(new HashMap<>(before.getParameters()));
    after.getParameters().put("transient_lastDdlTime", "999");
    NotificationEvent create = event(301L, MessageBuilder.CREATE_TABLE_EVENT,
        messageBuilder.buildCreateTableMessage(before, null).toString(),
        "hive", "sales", "orders");
    NotificationEvent alter = event(302L, MessageBuilder.ALTER_TABLE_EVENT,
        messageBuilder.buildAlterTableMessage(before, after, false, -1L).toString(),
        "hive", "sales", "orders");
    IMetaStoreClient client = mock(IMetaStoreClient.class);
    when(client.getNextNotification(any(NotificationEventRequest.class), eq(true), eq(null)))
        .thenReturn(response(create))
        .thenReturn(response(alter))
        .thenReturn(emptyResponse());

    CountDownLatch altered = new CountDownLatch(1);
    try (InMemorySearchFixture fixture = InMemorySearchFixture.create();
         MetastoreEventHandler handler = handler(conf, client)) {
      handler.addListeners(new MetastoreEventListener() {
        @Override
        public void notifyIndexTask(IndexTask task) throws IOException {
          apply(fixture, task);
          if (!task.tablesToDrop.isEmpty()) {
            altered.countDown();
          }
        }
      });

      handler.start(create.getEventId() - 1, 0);
      await(altered);
      assertFalse(fixture.searchMatch("sales", 5).isEmpty());
      Table loaded = fixture.loadTable("hive", "sales", "orders").hits().get(0).table();
      assertEquals("999", loaded.getParameters().get("transient_lastDdlTime"));
    }
  }

  @Test
  public void coalesceCreateThenDropInSameBatch() throws Exception {
    Configuration conf = eventHandlerConf();
    MessageBuilder messageBuilder = MessageBuilder.getInstance();
    Table table = InMemorySearchFixture.table("hive", "sales", "orders", "sales table");
    NotificationEvent create = event(201L, MessageBuilder.CREATE_TABLE_EVENT,
        messageBuilder.buildCreateTableMessage(table, null).toString(),
        "hive", "sales", "orders");
    NotificationEvent drop = event(202L, MessageBuilder.DROP_TABLE_EVENT,
        messageBuilder.buildDropTableMessage(table).toString(),
        "hive", "sales", "orders");
    IMetaStoreClient client = mock(IMetaStoreClient.class);
    when(client.getNextNotification(any(NotificationEventRequest.class), eq(true), eq(null)))
        .thenReturn(response(create, drop))
        .thenReturn(emptyResponse());

    CountDownLatch applied = new CountDownLatch(1);
    try (InMemorySearchFixture fixture = InMemorySearchFixture.create();
         MetastoreEventHandler handler = handler(conf, client)) {
      handler.addListeners(new MetastoreEventListener() {
        @Override
        public void notifyIndexTask(IndexTask task) throws IOException {
          apply(fixture, task);
          applied.countDown();
        }
      });

      handler.start(create.getEventId() - 1, 0);
      await(applied);
      assertTrue(fixture.searchMatch("sales", 5).isEmpty());
    }
  }

  @Test
  public void bootstrapWindowDoesNotRewindCursorWhenTailIsEmpty() throws Exception {
    Configuration conf = eventHandlerConf();
    conf.setLong(IndexOptions.POLL_NOTIFICATION_INTERVAL, 60_000L);
    NotificationEvent first = event(1L, "IGNORED_EVENT", "", "hive", "sales", "orders");
    NotificationEvent second = event(2L, "IGNORED_EVENT", "", "hive", "sales", "orders");
    CountDownLatch caughtUp = new CountDownLatch(1);
    IMetaStoreClient client = mock(IMetaStoreClient.class);
    when(client.getNextNotification(any(NotificationEventRequest.class), eq(true), eq(null)))
        .thenAnswer(invocation -> {
          NotificationEventRequest request = invocation.getArgument(0);
          if (request.getLastEvent() == 0L) {
            return response(first, second);
          }
          caughtUp.countDown();
          return emptyResponse();
        });

    List<Long> applied = new ArrayList<>();
    try (MetastoreEventHandler handler = handler(conf, client)) {
      handler.addListeners(new MetastoreEventListener() {
        @Override
        public void notifyIndexTask(IndexTask task) {
          applied.add(task.lastEventId);
        }
      });

      handler.start(0L, 2L);
      await(caughtUp);
      assertEquals(List.of(1L, 2L), applied);
      ArgumentCaptor<NotificationEventRequest> requests =
          ArgumentCaptor.forClass(NotificationEventRequest.class);
      verify(client, times(2)).getNextNotification(requests.capture(), eq(true), eq(null));
      assertEquals(0L, requests.getAllValues().get(0).getLastEvent());
      assertEquals(2L, requests.getAllValues().get(1).getLastEvent());
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
    return conf;
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
