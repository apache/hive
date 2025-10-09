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
import org.apache.hadoop.hive.metastore.messaging.MessageBuilder;

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
    register(MessageBuilder.ADD_PARTITION_EVENT, AddPartitionHandler.class);
    register(MessageBuilder.ALTER_DATABASE_EVENT, AlterDatabaseHandler.class);
    register(MessageBuilder.ALTER_PARTITION_EVENT, AlterPartitionHandler.class);
    register(MessageBuilder.ALTER_PARTITIONS_EVENT, AlterPartitionsHandler.class);
    register(MessageBuilder.ALTER_TABLE_EVENT, AlterTableHandler.class);
    register(MessageBuilder.CREATE_FUNCTION_EVENT, CreateFunctionHandler.class);
    register(MessageBuilder.CREATE_TABLE_EVENT, CreateTableHandler.class);
    register(MessageBuilder.DROP_PARTITION_EVENT, DropPartitionHandler.class);
    register(MessageBuilder.DROP_TABLE_EVENT, DropTableHandler.class);
    register(MessageBuilder.INSERT_EVENT, InsertHandler.class);
    register(MessageBuilder.DROP_FUNCTION_EVENT, DropFunctionHandler.class);
    register(MessageBuilder.ADD_PRIMARYKEY_EVENT, AddPrimaryKeyHandler.class);
    register(MessageBuilder.ADD_FOREIGNKEY_EVENT, AddForeignKeyHandler.class);
    register(MessageBuilder.ADD_UNIQUECONSTRAINT_EVENT, AddUniqueConstraintHandler.class);
    register(MessageBuilder.ADD_NOTNULLCONSTRAINT_EVENT, AddNotNullConstraintHandler.class);
    register(MessageBuilder.ADD_DEFAULTCONSTRAINT_EVENT, AddDefaultConstraintHandler.class);
    register(MessageBuilder.ADD_CHECKCONSTRAINT_EVENT, AddCheckConstraintHandler.class);
    register(MessageBuilder.DROP_CONSTRAINT_EVENT, DropConstraintHandler.class);
    register(MessageBuilder.CREATE_DATABASE_EVENT, CreateDatabaseHandler.class);
    register(MessageBuilder.DROP_DATABASE_EVENT, DropDatabaseHandler.class);
    register(MessageBuilder.OPEN_TXN_EVENT, OpenTxnHandler.class);
    register(MessageBuilder.COMMIT_TXN_EVENT, CommitTxnHandler.class);
    register(MessageBuilder.ABORT_TXN_EVENT, AbortTxnHandler.class);
    register(MessageBuilder.ALLOC_WRITE_ID_EVENT, AllocWriteIdHandler.class);
    register(MessageBuilder.UPDATE_TBL_COL_STAT_EVENT, UpdateTableColStatHandler.class);
    register(MessageBuilder.DELETE_TBL_COL_STAT_EVENT, DeleteTableColStatHandler.class);
    register(MessageBuilder.UPDATE_PART_COL_STAT_EVENT, UpdatePartColStatHandler.class);
    register(MessageBuilder.DELETE_PART_COL_STAT_EVENT, DeletePartColStatHandler.class);
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
