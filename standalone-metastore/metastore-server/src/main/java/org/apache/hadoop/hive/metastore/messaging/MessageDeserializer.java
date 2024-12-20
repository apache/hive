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
    case ADD_DEFAULTCONSTRAINT:
      return getAddDefaultConstraintMessage(messageBody);
    case ADD_CHECKCONSTRAINT:
      return getAddCheckConstraintMessage(messageBody);
    case DROP_CONSTRAINT:
      return getDropConstraintMessage(messageBody);
    case OPEN_TXN:
      return getOpenTxnMessage(messageBody);
    case COMMIT_TXN:
      return getCommitTxnMessage(messageBody);
    case ABORT_TXN:
      return getAbortTxnMessage(messageBody);
    case ALLOC_WRITE_ID:
      return getAllocWriteIdMessage(messageBody);
    case ACID_WRITE:
      return getAcidWriteMessage(messageBody);
    case UPDATE_TABLE_COLUMN_STAT:
      return getUpdateTableColumnStatMessage(messageBody);
    case DELETE_TABLE_COLUMN_STAT:
      return getDeleteTableColumnStatMessage(messageBody);
    case UPDATE_PARTITION_COLUMN_STAT:
      return getUpdatePartitionColumnStatMessage(messageBody);
    case DELETE_PARTITION_COLUMN_STAT:
      return getDeletePartitionColumnStatMessage(messageBody);
    case COMMIT_COMPACTION:
      return getCommitCompactionMessage(messageBody);
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

  public abstract AlterPartitionsMessage getAlterPartitionsMessage(String messageBody);

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
   * Method to de-serialize AddDefaultConstraintMessage instance.
   */
  public abstract AddDefaultConstraintMessage getAddDefaultConstraintMessage(String messageBody);

  /**
   * Method to de-serialize AddCheckConstraintMessage instance.
   */
  public abstract AddCheckConstraintMessage getAddCheckConstraintMessage(String messageBody);

  /**
   * Method to de-serialize DropConstraintMessage instance.
   */
  public abstract DropConstraintMessage getDropConstraintMessage(String messageBody);

  /**
   * Method to de-serialize OpenTxnMessage instance.
   */
  public abstract OpenTxnMessage getOpenTxnMessage(String messageBody);

  /**
   * Method to de-serialize CommitTxnMessage instance.
   */
  public abstract CommitTxnMessage getCommitTxnMessage(String messageBody);

  /**
   * Method to de-serialize AbortTxnMessage instance.
   */
  public abstract AbortTxnMessage getAbortTxnMessage(String messageBody);

  /*
   * Method to de-serialize AllocWriteIdMessage instance.
   */
  public abstract AllocWriteIdMessage getAllocWriteIdMessage(String messageBody);

  /*
   * Method to de-serialize AcidWriteMessage instance.
   */
  public abstract AcidWriteMessage getAcidWriteMessage(String messageBody);

  /**
   * Method to de-serialize UpdateTableColumnStatMessage instance.
   */
  public abstract UpdateTableColumnStatMessage getUpdateTableColumnStatMessage(String messageBody);

  /**
   * Method to de-serialize DeleteTableColumnStatMessage instance.
   */
  public abstract DeleteTableColumnStatMessage getDeleteTableColumnStatMessage(String messageBody);

  /**
   * Method to de-serialize UpdatePartitionColumnStatMessage instance.
   */
  public abstract UpdatePartitionColumnStatMessage getUpdatePartitionColumnStatMessage(String messageBody);

  /**
   * Method to de-serialize DeletePartitionColumnStatMessage instance.
   */
  public abstract DeletePartitionColumnStatMessage getDeletePartitionColumnStatMessage(String messageBody);

  /**
   * Method to de-serialize CommitCompactionMessage instance.
   */
  public abstract CommitCompactionMessage getCommitCompactionMessage(String messageBody);

  /**
   * Method to de-serialize any string passed. Need to be over-ridden by specific serialization subclasses.
   */
  public String deSerializeGenericString(String messageBody) {
    return messageBody;
  }

  // Protection against construction.
  protected MessageDeserializer() {}
}
