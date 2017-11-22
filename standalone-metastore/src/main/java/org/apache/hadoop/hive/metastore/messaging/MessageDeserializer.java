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

package org.apache.hadoop.hive.metastore.messaging;

/**
 * Interface for converting HCat events from String-form back to EventMessage instances.
 */
public abstract class MessageDeserializer {

  /**
   * Method to construct EventMessage from string.
   */
  public EventMessage getEventMessage(String eventTypeString, String messageBody) {

    switch (EventMessage.EventType.valueOf(eventTypeString)) {
    case CREATE_DATABASE:
      return getCreateDatabaseMessage(messageBody);
    case ALTER_DATABASE:
      return getAlterDatabaseMessage(messageBody);
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
    case CREATE_INDEX:
      return getCreateIndexMessage(messageBody);
    case DROP_INDEX:
      return getDropIndexMessage(messageBody);
    case ALTER_INDEX:
      return getAlterIndexMessage(messageBody);
    case INSERT:
      return getInsertMessage(messageBody);
    case ADD_PRIMARYKEY:
      return getAddPrimaryKeyMessage(messageBody);
    case ADD_FOREIGNKEY:
      return getAddForeignKeyMessage(messageBody);
    case ADD_UNIQUECONSTRAINT:
      return getAddUniqueConstraintMessage(messageBody);
    case ADD_NOTNULLCONSTRAINT:
      return getAddNotNullConstraintMessage(messageBody);
    case DROP_CONSTRAINT:
      return getDropConstraintMessage(messageBody);
    default:
      throw new IllegalArgumentException("Unsupported event-type: " + eventTypeString);
    }
  }

  /**
   * Method to de-serialize CreateDatabaseMessage instance.
   */
  public abstract CreateDatabaseMessage getCreateDatabaseMessage(String messageBody);

  /**
   * Method to de-serialize AlterDatabaseMessage instance.
   */
  public abstract AlterDatabaseMessage getAlterDatabaseMessage(String messageBody);

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
   * Method to de-serialize CreateIndexMessage instance.
   */
  public abstract CreateIndexMessage getCreateIndexMessage(String messageBody);

  /**
   * Method to de-serialize DropIndexMessage instance.
   */
  public abstract DropIndexMessage getDropIndexMessage(String messageBody);

  /**
   * Method to de-serialize AlterIndexMessage instance.
   */
  public abstract AlterIndexMessage getAlterIndexMessage(String messageBody);

  /**
   * Method to deserialize InsertMessage
   * @param messageBody the message in serialized form
   * @return message in object form
   */
  public abstract InsertMessage getInsertMessage(String messageBody);

  /**
   * Method to de-serialize AddPrimaryKeyMessage instance.
   */
  public abstract AddPrimaryKeyMessage getAddPrimaryKeyMessage(String messageBody);

  /**
   * Method to de-serialize AddForeignKeyMessage instance.
   */
  public abstract AddForeignKeyMessage getAddForeignKeyMessage(String messageBody);

  /**
   * Method to de-serialize AddUniqueConstraintMessage instance.
   */
  public abstract AddUniqueConstraintMessage getAddUniqueConstraintMessage(String messageBody);

  /**
   * Method to de-serialize AddNotNullConstraintMessage instance.
   */
  public abstract AddNotNullConstraintMessage getAddNotNullConstraintMessage(String messageBody);

  /**
   * Method to de-serialize DropConstraintMessage instance.
   */
  public abstract DropConstraintMessage getDropConstraintMessage(String messageBody);

  // Protection against construction.
  protected MessageDeserializer() {}
}
