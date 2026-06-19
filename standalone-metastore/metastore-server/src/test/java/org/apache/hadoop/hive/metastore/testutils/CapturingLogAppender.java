/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore.testutils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.message.Message;

/**
 * A log appender which captures the log events. This is useful for testing if log events
 * are being logged correctly and at correct levels.
 */
@Plugin(name = "CapturingLogAppender", category="Core", elementType="appender", printObject = true)
public class CapturingLogAppender extends AbstractAppender {
  private static List<LogEvent> EVENTS = new ArrayList<>();

  private CapturingLogAppender(String name, Filter filter, Layout<? extends Serializable> layout) {
    super(name, filter, layout);
  }

  @Override
  public void append(LogEvent logEvent) {
    EVENTS.add(logEvent);
  }

  public static List<String> findLogMessagesContaining(Level level, String substring) {
    return EVENTS.stream()
        .filter(event -> event.getLevel() == level)
        .map(LogEvent::getMessage)
        .map(Message::getFormattedMessage)
        .filter(msg -> msg.contains(substring))
        .collect(Collectors.toList());
  }

  @PluginFactory
  public static CapturingLogAppender createAppender(@PluginAttribute("name") String name) {
    return new CapturingLogAppender(name, null, null);
  }
}
