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
 * Class representing messages emitted when Metastore operations are done.
 * (E.g. Creation and deletion of databases, tables and partitions.)
 */
public abstract class EventMessage {

  /**
   * Enumeration of all supported types of Metastore operations.
   */
  public enum EventType {

    CREATE_DATABASE(MessageBuilder.CREATE_DATABASE_EVENT),
    DROP_DATABASE(MessageBuilder.DROP_DATABASE_EVENT),
    CREATE_TABLE(MessageBuilder.CREATE_TABLE_EVENT),
    DROP_TABLE(MessageBuilder.DROP_TABLE_EVENT),
    ADD_PARTITION(MessageBuilder.ADD_PARTITION_EVENT),
    DROP_PARTITION(MessageBuilder.DROP_PARTITION_EVENT),
    ALTER_DATABASE(MessageBuilder.ALTER_DATABASE_EVENT),
    ALTER_TABLE(MessageBuilder.ALTER_TABLE_EVENT),
    ALTER_PARTITION(MessageBuilder.ALTER_PARTITION_EVENT),
    INSERT(MessageBuilder.INSERT_EVENT),
    CREATE_FUNCTION(MessageBuilder.CREATE_FUNCTION_EVENT),
    DROP_FUNCTION(MessageBuilder.DROP_FUNCTION_EVENT),
    RELOAD(MessageBuilder.RELOAD_EVENT),

    ADD_PRIMARYKEY(MessageBuilder.ADD_PRIMARYKEY_EVENT),
    ADD_FOREIGNKEY(MessageBuilder.ADD_FOREIGNKEY_EVENT),
    ADD_UNIQUECONSTRAINT(MessageBuilder.ADD_UNIQUECONSTRAINT_EVENT),
    ADD_NOTNULLCONSTRAINT(MessageBuilder.ADD_NOTNULLCONSTRAINT_EVENT),
    ADD_DEFAULTCONSTRAINT(MessageBuilder.ADD_DEFAULTCONSTRAINT_EVENT),
    ADD_CHECKCONSTRAINT(MessageBuilder.ADD_CHECKCONSTRAINT_EVENT),
    DROP_CONSTRAINT(MessageBuilder.DROP_CONSTRAINT_EVENT),
    CREATE_ISCHEMA(MessageBuilder.CREATE_ISCHEMA_EVENT),
    ALTER_ISCHEMA(MessageBuilder.ALTER_ISCHEMA_EVENT),
    DROP_ISCHEMA(MessageBuilder.DROP_ISCHEMA_EVENT),
    ADD_SCHEMA_VERSION(MessageBuilder.ADD_SCHEMA_VERSION_EVENT),
    ALTER_SCHEMA_VERSION(MessageBuilder.ALTER_SCHEMA_VERSION_EVENT),
    DROP_SCHEMA_VERSION(MessageBuilder.DROP_SCHEMA_VERSION_EVENT),
    CREATE_CATALOG(MessageBuilder.CREATE_CATALOG_EVENT),
    DROP_CATALOG(MessageBuilder.DROP_CATALOG_EVENT),
    OPEN_TXN(MessageBuilder.OPEN_TXN_EVENT),
    COMMIT_TXN(MessageBuilder.COMMIT_TXN_EVENT),
    ABORT_TXN(MessageBuilder.ABORT_TXN_EVENT),
    ALLOC_WRITE_ID(MessageBuilder.ALLOC_WRITE_ID_EVENT),
    ALTER_CATALOG(MessageBuilder.ALTER_CATALOG_EVENT),
    ACID_WRITE(MessageBuilder.ACID_WRITE_EVENT),
    BATCH_ACID_WRITE(MessageBuilder.BATCH_ACID_WRITE_EVENT),
    UPDATE_TABLE_COLUMN_STAT(MessageBuilder.UPDATE_TBL_COL_STAT_EVENT),
    DELETE_TABLE_COLUMN_STAT(MessageBuilder.DELETE_TBL_COL_STAT_EVENT),
    UPDATE_PARTITION_COLUMN_STAT(MessageBuilder.UPDATE_PART_COL_STAT_EVENT),
    UPDATE_PARTITION_COLUMN_STAT_BATCH(MessageBuilder.UPDATE_PART_COL_STAT_EVENT_BATCH),
    DELETE_PARTITION_COLUMN_STAT(MessageBuilder.DELETE_PART_COL_STAT_EVENT),
    COMMIT_COMPACTION(MessageBuilder.COMMIT_COMPACTION_EVENT),
    CREATE_DATACONNECTOR(MessageBuilder.CREATE_DATACONNECTOR_EVENT),
    DROP_DATACONNECTOR(MessageBuilder.DROP_DATACONNECTOR_EVENT),
    ALTER_DATACONNECTOR(MessageBuilder.ALTER_DATACONNECTOR_EVENT);

    private String typeString;

    EventType(String typeString) {
      this.typeString = typeString;
    }

    @Override
    public String toString() { return typeString; }
  }

  protected EventType eventType;

  protected EventMessage(EventType eventType) {
    this.eventType = eventType;
  }

  public EventType getEventType() {
    return eventType;
  }

  /**
   * Getter for HCatalog Server's URL.
   * (This is where the event originates from.)
   * @return HCatalog Server's URL (String).
   */
  public abstract String getServer();

  /**
   * Getter for the Kerberos principal of the HCatalog service.
   * @return HCatalog Service Principal (String).
   */
  public abstract String getServicePrincipal();

  /**
   * Getter for the name of the Database on which the Metastore operation is done.
   * @return Database-name (String).
   */
  public abstract String getDB();

  /**
   * Getter for the timestamp associated with the operation.
   * @return Timestamp (Long - seconds since epoch).
   */
  public abstract Long   getTimestamp();

  /**
   * Class invariant. Checked after construction or deserialization.
   */
  public EventMessage checkValid() {
    if (getServer() == null || getServicePrincipal() == null) {
      throw new IllegalStateException("Server-URL/Service-Principal shouldn't be null.");
    }
    if (getEventType() == null) {
      throw new IllegalStateException("Event-type unset.");
    }
    if (getDB() == null) {
      throw new IllegalArgumentException("DB-name unset.");
    }

    return this;
  }
}
