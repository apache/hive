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

package org.apache.hive.hcatalog.messaging;

import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.common.classification.InterfaceStability;

/**
 * Interface for converting HCat events from String-form back to HCatEventMessage instances.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class MessageDeserializer {

  /**
   * Method to construct HCatEventMessage from string.
   */
  public HCatEventMessage getHCatEventMessage(String eventTypeString, String messageBody) {

    switch (HCatEventMessage.EventType.valueOf(eventTypeString)) {
    case CREATE_DATABASE:
      return getCreateDatabaseMessage(messageBody);
    case DROP_DATABASE:
      return getDropDatabaseMessage(messageBody);
    case CREATE_TABLE:
      return getCreateTableMessage(messageBody);
    case ALTER_TABLE:
      return getAlterTableMessage(messageBody);
    case DROP_TABLE:
      return getDropTableMessage(messageBody);
    case ADD_PARTITION:
      return getAddPartitionMessage(messageBody);
    case ALTER_PARTITION:
      return getAlterPartitionMessage(messageBody);
    case DROP_PARTITION:
      return getDropPartitionMessage(messageBody);
    case CREATE_FUNCTION:
      return getCreateFunctionMessage(messageBody);
    case DROP_FUNCTION:
      return getDropFunctionMessage(messageBody);
    case INSERT:
      return getInsertMessage(messageBody);
    default:
      throw new IllegalArgumentException("Unsupported event-type: " + eventTypeString);
    }
  }

  /**
   * Method to de-serialize CreateDatabaseMessage instance.
   */
  public abstract CreateDatabaseMessage getCreateDatabaseMessage(String messageBody);

  /**
   * Method to de-serialize DropDatabaseMessage instance.
   */
  public abstract DropDatabaseMessage getDropDatabaseMessage(String messageBody);

  /**
   * Method to de-serialize CreateTableMessage instance.
   */
  public abstract CreateTableMessage getCreateTableMessage(String messageBody);

  /**
   * Method to de-serialize AlterTableMessge
   * @param messageBody string message
   * @return object message
   */
  public abstract AlterTableMessage getAlterTableMessage(String messageBody);

  /**
   * Method to de-serialize DropTableMessage instance.
   */
  public abstract DropTableMessage getDropTableMessage(String messageBody);

  /**
   * Method to de-serialize AddPartitionMessage instance.
   */
  public abstract AddPartitionMessage getAddPartitionMessage(String messageBody);

  /**
   * Method to deserialize AlterPartitionMessage
   * @param messageBody the message in serialized form
   * @return message in object form
   */
  public abstract AlterPartitionMessage getAlterPartitionMessage(String messageBody);

  /**
   * Method to de-serialize DropPartitionMessage instance.
   */
  public abstract DropPartitionMessage getDropPartitionMessage(String messageBody);

  /**
   * Method to de-serialize CreateFunctionMessage instance.
   */
  public abstract CreateFunctionMessage getCreateFunctionMessage(String messageBody);

  /**
   * Method to de-serialize DropFunctionMessage instance.
   */
  public abstract DropFunctionMessage getDropFunctionMessage(String messageBody);

  /**
   * Method to deserialize InsertMessage
   * @param messageBody the message in serialized form
   * @return message in object form
   */
  public abstract InsertMessage getInsertMessage(String messageBody);

  // Protection against construction.
  protected MessageDeserializer() {}
}
