/**
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

/**
 * Interface for converting HCat events from String-form back to HCatEventMessage instances.
 */
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
    case DROP_TABLE:
      return getDropTableMessage(messageBody);
    case ADD_PARTITION:
      return getAddPartitionMessage(messageBody);
    case DROP_PARTITION:
      return getDropPartitionMessage(messageBody);

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
   * Method to de-serialize DropTableMessage instance.
   */
  public abstract DropTableMessage getDropTableMessage(String messageBody);

  /**
   * Method to de-serialize AddPartitionMessage instance.
   */
  public abstract AddPartitionMessage getAddPartitionMessage(String messageBody);

  /**
   * Method to de-serialize DropPartitionMessage instance.
   */
  public abstract DropPartitionMessage getDropPartitionMessage(String messageBody);

  // Protection against construction.
  protected MessageDeserializer() {}
}
