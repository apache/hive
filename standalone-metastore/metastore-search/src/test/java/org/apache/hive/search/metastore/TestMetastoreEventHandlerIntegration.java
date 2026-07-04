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
import org.apache.hive.search.search.InMemorySearchFixture;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Category(MetastoreUnitTest.class)
public class TestMetastoreEventHandlerIntegration {

  @Test
  public void pollsNotificationsAndUpdatesIndex() throws Exception {
    Configuration conf = new Configuration(false);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.EVENT_MESSAGE_FACTORY,
        JSONMessageEncoder.class.getName());
    conf.setLong(IndexConfig.EVENT_FAILURE_BACKOFF_MS, 0L);
    conf.setInt(IndexConfig.EVENT_BATCH_MAX_FAILURES, 1);

    MessageBuilder messageBuilder = MessageBuilder.getInstance();
    Table salesOrders = InMemorySearchFixture.table("hive", "sales", "orders", "daily sales orders");
    String createMessage = messageBuilder.buildCreateTableMessage(salesOrders, null).toString();

    NotificationEvent createEvent = event(101L, MessageBuilder.CREATE_TABLE_EVENT, createMessage,
        "hive", "sales", "orders");
    NotificationEvent dropEvent = event(
        102L,
        MessageBuilder.DROP_TABLE_EVENT,
        messageBuilder.buildDropTableMessage(salesOrders).toString(),
        "hive",
        "sales",
        "orders");

    IMetaStoreClient client = mock(IMetaStoreClient.class);
    when(client.getNextNotification(any(NotificationEventRequest.class), eq(true), eq(null)))
        .thenReturn(response(createEvent))
        .thenReturn(response(dropEvent))
        .thenReturn(emptyResponse());

    try (InMemorySearchFixture fixture = InMemorySearchFixture.create()) {
      AtomicReference<MetastoreEventListener.IndexTask> lastTask = new AtomicReference<>();
      MetastoreEventHandler handler = new MetastoreEventHandler(conf, client);
      handler.addListeners(new MetastoreEventListener() {
        @Override
        public void notifyIndexTask(IndexTask task) throws IOException {
          lastTask.set(task);
          try {
            fixture.mutations().apply(task);
            fixture.commit(task.lastEventId);
          } catch (Exception e) {
            throw new IOException(e);
          }
        }
      });

      assertEquals(1, handler.getNextMetastoreEvents(10));
      assertEquals(1, lastTask.get().tablesToAdd.size());
      assertFalse(fixture.searchMatch("sales", 5).isEmpty());

      assertEquals(1, handler.getNextMetastoreEvents(10));
      assertEquals(1, lastTask.get().tablesToDrop.size());
      assertTrue(fixture.searchMatch("sales", 5).isEmpty());
    }
  }

  @Test
  public void coalesceCreateThenDropInSameBatch() throws Exception {
    Configuration conf = new Configuration(false);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.EVENT_MESSAGE_FACTORY,
        JSONMessageEncoder.class.getName());
    MessageBuilder messageBuilder = MessageBuilder.getInstance();
    Table table = InMemorySearchFixture.table("hive", "sales", "orders", "sales table");
    NotificationEvent create = event(
        201L,
        MessageBuilder.CREATE_TABLE_EVENT,
        messageBuilder.buildCreateTableMessage(table, null).toString(),
        "hive",
        "sales",
        "orders");
    NotificationEvent drop = event(
        202L,
        MessageBuilder.DROP_TABLE_EVENT,
        messageBuilder.buildDropTableMessage(table).toString(),
        "hive",
        "sales",
        "orders");

    IMetaStoreClient client = mock(IMetaStoreClient.class);
    when(client.getNextNotification(any(NotificationEventRequest.class), eq(true), eq(null)))
        .thenReturn(response(create, drop))
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
      assertTrue(fixture.searchMatch("sales", 5).isEmpty());
    }
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
