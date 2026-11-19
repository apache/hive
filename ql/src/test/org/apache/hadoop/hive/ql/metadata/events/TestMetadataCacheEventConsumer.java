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

package org.apache.hadoop.hive.ql.metadata.events;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.messaging.EventMessage;
import org.apache.hadoop.hive.metastore.messaging.MessageBuilder;
import org.apache.hadoop.hive.metastore.messaging.json.JSONMessageEncoder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestMetadataCacheEventConsumer {
  private final TableVersionRegistry registry = TableVersionRegistry.get();
  private final MetadataCacheEventConsumer consumer = new MetadataCacheEventConsumer(registry);
  private long nextEventId = 1;

  @Before
  public void setUp() {
    registry.enable(new HiveConf());
  }

  @After
  public void tearDown() {
    registry.disable();
  }

  @Test
  public void testTableEvents() {
    for (String type : new String[] {MessageBuilder.ADD_PARTITION_EVENT, MessageBuilder.ALTER_PARTITION_EVENT,
        MessageBuilder.DROP_PARTITION_EVENT, MessageBuilder.INSERT_EVENT, MessageBuilder.UPDATE_TBL_COL_STAT_EVENT,
        MessageBuilder.UPDATE_PART_COL_STAT_EVENT, MessageBuilder.ADD_PRIMARYKEY_EVENT,
        MessageBuilder.DROP_CONSTRAINT_EVENT, MessageBuilder.DROP_TABLE_EVENT, MessageBuilder.CREATE_TABLE_EVENT}) {
      long before = version("t");
      long other = version("other");
      consumer.accept(event(type, "db", "t", "{}"));
      assertNotEquals(type, before, version("t"));
      assertEquals(type, other, version("other"));
    }
  }

  @Test
  public void testRenameInvalidatesBothNames() {
    long oldName = version("old_t");
    long newName = version("new_t");
    String msg = serialize(MessageBuilder.getInstance().buildAlterTableMessage(table("old_t"), table("new_t"),
        false, null));
    consumer.accept(event(MessageBuilder.ALTER_TABLE_EVENT, "db", "new_t", msg));
    assertNotEquals(oldName, version("old_t"));
    assertNotEquals(newName, version("new_t"));
  }

  @Test
  public void testForeignKeyInvalidatesParent() {
    long parent = version("parent");
    SQLForeignKey fk = new SQLForeignKey("db", "parent", "id", "db", "child", "parent_id", 1, 0, 0, "fk", "pk",
        false, false, false);
    fk.setCatName("hive");
    String msg = serialize(MessageBuilder.getInstance().buildAddForeignKeyMessage(Collections.singletonList(fk)));
    consumer.accept(event(MessageBuilder.ADD_FOREIGNKEY_EVENT, "db", "child", msg));
    assertNotEquals(parent, version("parent"));
  }

  @Test
  public void testDatabaseEvent() {
    long t = version("t");
    consumer.accept(event(MessageBuilder.ALTER_DATABASE_EVENT, "db", null, "{}"));
    assertNotEquals(t, version("t"));
  }

  @Test
  public void testIgnoredEvent() {
    long t = version("t");
    consumer.accept(event(MessageBuilder.OPEN_TXN_EVENT, null, null, "{}"));
    consumer.accept(event(MessageBuilder.CREATE_FUNCTION_EVENT, "db", null, "{}"));
    assertEquals(t, version("t"));
  }

  @Test
  public void testUnknownOrUnreadableEventInvalidatesAll() {
    long t = version("t");
    consumer.accept(event("SOME_NEW_EVENT", "db", "x", "{}"));
    long t2 = version("t");
    assertNotEquals(t, t2);
    // an ALTER_TABLE message that cannot be read
    consumer.accept(event(MessageBuilder.ALTER_TABLE_EVENT, "db", "x", "not json"));
    assertNotEquals(t2, version("t"));
    // a table event without a table name
    long t3 = version("t");
    consumer.accept(event(MessageBuilder.INSERT_EVENT, "db", null, "{}"));
    assertNotEquals(t3, version("t"));
  }

  /** Every event type the metastore has is either handled or knowingly ignored, not taken as unknown. */
  @Test
  public void testAllEventTypesClassified() {
    for (EventMessage.EventType eventType : EventMessage.EventType.values()) {
      String type = eventType.toString();
      if (type.equals(MessageBuilder.ALTER_TABLE_EVENT) || type.equals(MessageBuilder.ADD_FOREIGNKEY_EVENT) ||
          type.equals(MessageBuilder.COMMIT_TXN_EVENT)) {
        // they need a message, see the tests above
        continue;
      }
      long other = version("other");
      consumer.accept(event(type, "db", "t", "{}"));
      if (!type.endsWith("DATABASE")) {
        assertEquals("event type " + type + " not classified", other, version("other"));
      }
    }
  }

  @Test
  public void testPollAppliesEventsAndRecoversFromMissingOnes() throws Exception {
    FakeFetcher fetcher = new FakeFetcher();
    HiveConf conf = new HiveConf();
    conf.setTimeVar(HiveConf.ConfVars.HIVE_NOTFICATION_EVENT_POLL_INTERVAL, 1, TimeUnit.HOURS);
    conf.setVar(HiveConf.ConfVars.HIVE_NOTFICATION_EVENT_CONSUMERS, MetadataCacheEventConsumer.class.getName());
    NotificationEventPoll poll = new NotificationEventPoll(conf, fetcher);
    try {
      assertTrue(registry.isEnabled());
      long t = version("t");

      fetcher.add(event(MessageBuilder.INSERT_EVENT, "db", "t", "{}"));
      poll.requestSync().get(10, TimeUnit.SECONDS);
      assertEquals(fetcher.latest(), poll.lastCheckedEventId);
      long t2 = version("t");
      assertNotEquals(t, t2);

      // the cleaner removed events the poll did not read yet
      fetcher.add(event(MessageBuilder.CREATE_FUNCTION_EVENT, "db", null, "{}"));
      fetcher.add(event(MessageBuilder.CREATE_FUNCTION_EVENT, "db", null, "{}"));
      fetcher.events.remove(fetcher.events.size() - 2);
      poll.requestSync().get(10, TimeUnit.SECONDS);
      assertEquals(fetcher.latest(), poll.lastCheckedEventId);
      assertNotEquals("everything invalidated", t2, version("t"));
    } finally {
      poll.stop();
    }
  }

  private long version(String tbl) {
    return registry.getVersion("hive", "db", tbl, 0);
  }

  private NotificationEvent event(String type, String db, String tbl, String message) {
    NotificationEvent event = new NotificationEvent(nextEventId++, 0, type, message);
    event.setMessageFormat(JSONMessageEncoder.FORMAT);
    event.setCatName("hive");
    event.setDbName(db);
    event.setTableName(tbl);
    return event;
  }

  private static String serialize(EventMessage msg) {
    return JSONMessageEncoder.getInstance().getSerializer().serialize(msg);
  }

  private static Table table(String name) {
    Table table = new Table();
    table.setCatName("hive");
    table.setDbName("db");
    table.setTableName(name);
    table.setParameters(new HashMap<>());
    return table;
  }

  /** The notification log, failing as the metastore client does when ids are missing. */
  private static class FakeFetcher implements EventUtils.NotificationFetcher, Supplier<EventUtils.NotificationFetcher> {
    final List<NotificationEvent> events = new ArrayList<>();

    void add(NotificationEvent event) {
      events.add(event);
    }

    long latest() {
      return events.isEmpty() ? 0 : events.get(events.size() - 1).getEventId();
    }

    @Override
    public EventUtils.NotificationFetcher get() {
      return this;
    }

    @Override
    public int getBatchSize() {
      return 10;
    }

    @Override
    public long getCurrentNotificationEventId() {
      return latest();
    }

    @Override
    public long getDbNotificationEventsCount(long fromEventId, String dbName, Long toEventId, int limit) {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<NotificationEvent> getNextNotificationEvents(long pos, IMetaStoreClient.NotificationFilter filter) {
      List<NotificationEvent> result = new ArrayList<>();
      long expected = pos + 1;
      for (NotificationEvent e : events) {
        if (e.getEventId() > pos && result.size() < getBatchSize()) {
          if (e.getEventId() != expected) {
            throw new IllegalStateException("Notification events are missing in the meta store.");
          }
          result.add(e);
          expected++;
        }
      }
      return result;
    }
  }
}
