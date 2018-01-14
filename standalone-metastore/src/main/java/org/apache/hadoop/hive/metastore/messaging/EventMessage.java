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

    CREATE_DATABASE(MessageFactory.CREATE_DATABASE_EVENT),
    DROP_DATABASE(MessageFactory.DROP_DATABASE_EVENT),
    CREATE_TABLE(MessageFactory.CREATE_TABLE_EVENT),
    DROP_TABLE(MessageFactory.DROP_TABLE_EVENT),
    ADD_PARTITION(MessageFactory.ADD_PARTITION_EVENT),
    DROP_PARTITION(MessageFactory.DROP_PARTITION_EVENT),
    ALTER_DATABASE(MessageFactory.ALTER_DATABASE_EVENT),
    ALTER_TABLE(MessageFactory.ALTER_TABLE_EVENT),
    ALTER_PARTITION(MessageFactory.ALTER_PARTITION_EVENT),
    INSERT(MessageFactory.INSERT_EVENT),
    CREATE_FUNCTION(MessageFactory.CREATE_FUNCTION_EVENT),
    DROP_FUNCTION(MessageFactory.DROP_FUNCTION_EVENT),
    CREATE_INDEX(MessageFactory.CREATE_INDEX_EVENT),
    DROP_INDEX(MessageFactory.DROP_INDEX_EVENT),
    ALTER_INDEX(MessageFactory.ALTER_INDEX_EVENT),
    ADD_PRIMARYKEY(MessageFactory.ADD_PRIMARYKEY_EVENT),
    ADD_FOREIGNKEY(MessageFactory.ADD_FOREIGNKEY_EVENT),
    ADD_UNIQUECONSTRAINT(MessageFactory.ADD_UNIQUECONSTRAINT_EVENT),
    ADD_NOTNULLCONSTRAINT(MessageFactory.ADD_NOTNULLCONSTRAINT_EVENT),
    DROP_CONSTRAINT(MessageFactory.DROP_CONSTRAINT_EVENT);

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
    if (getServer() == null || getServicePrincipal() == null)
      throw new IllegalStateException("Server-URL/Service-Principal shouldn't be null.");
    if (getEventType() == null)
      throw new IllegalStateException("Event-type unset.");
    if (getDB() == null)
      throw new IllegalArgumentException("DB-name unset.");

    return this;
  }
}
