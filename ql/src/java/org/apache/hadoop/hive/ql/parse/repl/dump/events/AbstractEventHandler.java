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
import org.apache.hadoop.hive.metastore.messaging.MessageDeserializer;
import org.apache.hadoop.hive.metastore.messaging.MessageFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class AbstractEventHandler implements EventHandler {
  static final Logger LOG = LoggerFactory.getLogger(AbstractEventHandler.class);

  final NotificationEvent event;
  final MessageDeserializer deserializer;

  AbstractEventHandler(NotificationEvent event) {
    this.event = event;
    deserializer = MessageFactory.getInstance().getDeserializer();
  }

  @Override
  public long fromEventId() {
    return event.getEventId();
  }

  @Override
  public long toEventId() {
    return event.getEventId();
  }
}
