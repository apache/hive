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

package org.apache.hadoop.hive.metastore.messaging.json;

import org.apache.hadoop.hive.metastore.messaging.AddPartitionMessage;
import org.apache.hadoop.hive.metastore.messaging.AlterIndexMessage;
import org.apache.hadoop.hive.metastore.messaging.AlterPartitionMessage;
import org.apache.hadoop.hive.metastore.messaging.AlterTableMessage;
import org.apache.hadoop.hive.metastore.messaging.CreateDatabaseMessage;
import org.apache.hadoop.hive.metastore.messaging.CreateFunctionMessage;
import org.apache.hadoop.hive.metastore.messaging.CreateIndexMessage;
import org.apache.hadoop.hive.metastore.messaging.CreateTableMessage;
import org.apache.hadoop.hive.metastore.messaging.DropDatabaseMessage;
import org.apache.hadoop.hive.metastore.messaging.DropFunctionMessage;
import org.apache.hadoop.hive.metastore.messaging.DropIndexMessage;
import org.apache.hadoop.hive.metastore.messaging.DropPartitionMessage;
import org.apache.hadoop.hive.metastore.messaging.DropTableMessage;
import org.apache.hadoop.hive.metastore.messaging.InsertMessage;
import org.apache.hadoop.hive.metastore.messaging.MessageDeserializer;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;

/**
 * MessageDeserializer implementation, for deserializing from JSON strings.
 */
public class JSONMessageDeserializer extends MessageDeserializer {

  static ObjectMapper mapper = new ObjectMapper(); // Thread-safe.

  static {
    mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    mapper.configure(SerializationConfig.Feature.AUTO_DETECT_GETTERS, false);
    mapper.configure(SerializationConfig.Feature.AUTO_DETECT_IS_GETTERS, false);
    mapper.configure(SerializationConfig.Feature.AUTO_DETECT_FIELDS, false);
  }

  @Override
  public CreateDatabaseMessage getCreateDatabaseMessage(String messageBody) {
    try {
      return mapper.readValue(messageBody, JSONCreateDatabaseMessage.class);
    }
    catch (Exception exception) {
      throw new IllegalArgumentException("Could not construct JSONCreateDatabaseMessage.", exception);
    }
  }

  @Override
  public DropDatabaseMessage getDropDatabaseMessage(String messageBody) {
    try {
      return mapper.readValue(messageBody, JSONDropDatabaseMessage.class);
    }
    catch (Exception exception) {
      throw new IllegalArgumentException("Could not construct JSONDropDatabaseMessage.", exception);
    }
  }

  @Override
  public CreateTableMessage getCreateTableMessage(String messageBody) {
    try {
      return mapper.readValue(messageBody, JSONCreateTableMessage.class);
    }
    catch (Exception exception) {
      throw new IllegalArgumentException("Could not construct JSONCreateTableMessage.", exception);
    }
  }

  @Override
  public AlterTableMessage getAlterTableMessage(String messageBody) {
    try {
      return mapper.readValue(messageBody, JSONAlterTableMessage.class);
    }
    catch (Exception exception) {
      throw new IllegalArgumentException("Could not construct appropriate alter table type.",
          exception);
    }
  }

  @Override
  public DropTableMessage getDropTableMessage(String messageBody) {
    try {
      return mapper.readValue(messageBody, JSONDropTableMessage.class);
    }
    catch (Exception exception) {
      throw new IllegalArgumentException("Could not construct JSONDropTableMessage.", exception);
    }
  }

  @Override
  public AddPartitionMessage getAddPartitionMessage(String messageBody) {
    try {
      return mapper.readValue(messageBody, JSONAddPartitionMessage.class);
    }
    catch (Exception exception) {
      throw new IllegalArgumentException("Could not construct AddPartitionMessage.", exception);
    }
  }

  @Override
  public AlterPartitionMessage getAlterPartitionMessage(String messageBody) {
    try {
      return mapper.readValue(messageBody, JSONAlterPartitionMessage.class);
    } catch (Exception e) {
      throw new IllegalArgumentException("Could not construct AlterPartitionMessage.", e);
    }
  }

  @Override
  public DropPartitionMessage getDropPartitionMessage(String messageBody) {
    try {
      return mapper.readValue(messageBody, JSONDropPartitionMessage.class);
    }
    catch (Exception exception) {
      throw new IllegalArgumentException("Could not construct DropPartitionMessage.", exception);
    }
  }

  @Override
  public CreateFunctionMessage getCreateFunctionMessage(String messageBody) {
    try {
      return mapper.readValue(messageBody, JSONCreateFunctionMessage.class);
    }
    catch (Exception exception) {
      throw new IllegalArgumentException("Could not construct JSONCreateFunctionMessage.", exception);
    }
  }

  @Override
  public DropFunctionMessage getDropFunctionMessage(String messageBody) {
    try {
      return mapper.readValue(messageBody, JSONDropFunctionMessage.class);
    }
    catch (Exception exception) {
      throw new IllegalArgumentException("Could not construct JSONDropDatabaseMessage.", exception);
    }
  }

  @Override
  public CreateIndexMessage getCreateIndexMessage(String messageBody) {
    try {
      return mapper.readValue(messageBody, JSONCreateIndexMessage.class);
    }
    catch (Exception exception) {
      throw new IllegalArgumentException("Could not construct JSONCreateIndexMessage.", exception);
    }
  }

  @Override
  public DropIndexMessage getDropIndexMessage(String messageBody) {
    try {
      return mapper.readValue(messageBody, JSONDropIndexMessage.class);
    }
    catch (Exception exception) {
      throw new IllegalArgumentException("Could not construct JSONDropIndexMessage.", exception);
    }
  }

  @Override
  public AlterIndexMessage getAlterIndexMessage(String messageBody) {
    try {
      return mapper.readValue(messageBody, JSONAlterIndexMessage.class);
    }
    catch (Exception exception) {
      throw new IllegalArgumentException("Could not construct JSONAlterIndexMessage.", exception);
    }
  }

  @Override
  public InsertMessage getInsertMessage(String messageBody) {
    try {
      return mapper.readValue(messageBody, JSONInsertMessage.class);
    } catch (Exception e) {
      throw new IllegalArgumentException("Could not construct InsertMessage", e);
    }
  }
}
