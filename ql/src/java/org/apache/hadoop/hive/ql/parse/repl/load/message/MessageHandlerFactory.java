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
package org.apache.hadoop.hive.ql.parse.repl.load.message;

import org.apache.hadoop.hive.metastore.api.NotificationEvent;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.ql.parse.repl.DumpType;

public class MessageHandlerFactory {
  private static Map<DumpType, Class<? extends MessageHandler>> messageHandlers = new HashMap<>();

  static {
    register(DumpType.EVENT_DROP_PARTITION, DropPartitionHandler.class);
    register(DumpType.EVENT_DROP_TABLE, DropTableHandler.class);
    register(DumpType.EVENT_INSERT, InsertHandler.class);
    register(DumpType.EVENT_RENAME_PARTITION, RenamePartitionHandler.class);
    register(DumpType.EVENT_RENAME_TABLE, RenameTableHandler.class);

    register(DumpType.EVENT_CREATE_TABLE, TableHandler.class);
    register(DumpType.EVENT_ADD_PARTITION, TableHandler.class);
    register(DumpType.EVENT_ALTER_TABLE, TableHandler.class);
    register(DumpType.EVENT_ALTER_PARTITION, TableHandler.class);

    register(DumpType.EVENT_TRUNCATE_PARTITION, TruncatePartitionHandler.class);
    register(DumpType.EVENT_TRUNCATE_TABLE, TruncateTableHandler.class);
  }

  private static void register(DumpType eventType, Class<? extends MessageHandler> handlerClazz) {
    try {
      Constructor<? extends MessageHandler> constructor =
          handlerClazz.getDeclaredConstructor();
      assert constructor != null;
      assert !Modifier.isPrivate(constructor.getModifiers());
      messageHandlers.put(eventType, handlerClazz);
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException("handler class: " + handlerClazz.getCanonicalName()
          + " does not have the a constructor with only parameter of type:"
          + NotificationEvent.class.getCanonicalName(), e);
    }
  }

  public static MessageHandler handlerFor(DumpType eventType) {
    if (messageHandlers.containsKey(eventType)) {
      Class<? extends MessageHandler> handlerClazz = messageHandlers.get(eventType);
      try {
        Constructor<? extends MessageHandler> constructor =
            handlerClazz.getDeclaredConstructor();
        return constructor.newInstance();
      } catch (NoSuchMethodException | IllegalAccessException | InstantiationException | InvocationTargetException e) {
        // this should never happen. however we want to make sure we propagate the exception
        throw new RuntimeException(
            "failed when creating handler for " + eventType
                + " with the responsible class being " + handlerClazz.getCanonicalName(), e);
      }
    }
    return new DefaultHandler();
  }
}
