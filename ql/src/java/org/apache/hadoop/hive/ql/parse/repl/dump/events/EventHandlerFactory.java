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
import org.apache.hadoop.hive.metastore.messaging.MessageFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;

public class EventHandlerFactory {
  private EventHandlerFactory() {
  }

  private static Map<String, Class<? extends EventHandler>> registeredHandlers = new HashMap<>();

  static {
    register(MessageFactory.ADD_PARTITION_EVENT, AddPartitionHandler.class);
    register(MessageFactory.ALTER_PARTITION_EVENT, AlterPartitionHandler.class);
    register(MessageFactory.ALTER_TABLE_EVENT, AlterTableHandler.class);
    register(MessageFactory.CREATE_FUNCTION_EVENT, CreateFunctionHandler.class);
    register(MessageFactory.CREATE_TABLE_EVENT, CreateTableHandler.class);
    register(MessageFactory.DROP_PARTITION_EVENT, DropPartitionHandler.class);
    register(MessageFactory.DROP_TABLE_EVENT, DropTableHandler.class);
    register(MessageFactory.INSERT_EVENT, InsertHandler.class);
  }

  static void register(String event, Class<? extends EventHandler> handlerClazz) {
    try {
      Constructor<? extends EventHandler> constructor =
          handlerClazz.getDeclaredConstructor(NotificationEvent.class);
      assert constructor != null;
      assert !Modifier.isPrivate(constructor.getModifiers());
      registeredHandlers.put(event, handlerClazz);
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException("handler class: " + handlerClazz.getCanonicalName()
          + " does not have the a constructor with only parameter of type:"
          + NotificationEvent.class.getCanonicalName(), e);
    }
  }

  public static EventHandler handlerFor(NotificationEvent event) {
    if (registeredHandlers.containsKey(event.getEventType())) {
      Class<? extends EventHandler> handlerClazz = registeredHandlers.get(event.getEventType());
      try {
        Constructor<? extends EventHandler> constructor =
            handlerClazz.getDeclaredConstructor(NotificationEvent.class);
        return constructor.newInstance(event);
      } catch (NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
        // this should never happen. however we want to make sure we propagate the exception
        throw new RuntimeException(
            "failed when creating handler for " + event.getEventType()
                + " with the responsible class being " + handlerClazz.getCanonicalName(), e);
      }
    }
    return new DefaultHandler(event);
  }
}
