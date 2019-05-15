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

package org.apache.hadoop.hive.ql.parse.repl.dump.events;

import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.messaging.json.JSONMessageEncoder;
import org.apache.hadoop.hive.ql.parse.repl.DumpType;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class TestEventHandlerFactory {
  @Test(expected = IllegalArgumentException.class)
  public void shouldNotAllowRegisteringEventsWhichCannotBeInstantiated() {
    class NonCompatibleEventHandler implements EventHandler {
      @Override
      public void handle(Context withinContext) throws Exception {

      }

      @Override
      public long fromEventId() {
        return 0;
      }

      @Override
      public long toEventId() {
        return 0;
      }

      @Override
      public DumpType dumpType() {
        return null;
      }
    }
    EventHandlerFactory.register("anyEvent", NonCompatibleEventHandler.class);
  }

  @Test
  public void shouldProvideDefaultHandlerWhenNothingRegisteredForThatEvent() {
    NotificationEvent event = new NotificationEvent(Long.MAX_VALUE, Integer.MAX_VALUE,
        "shouldGiveDefaultHandler", "s");
    event.setMessageFormat(JSONMessageEncoder.FORMAT);
    EventHandler eventHandler =
        EventHandlerFactory.handlerFor(event);
    assertTrue(eventHandler instanceof DefaultHandler);
  }

}
