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

package org.apache.hcatalog.messaging.json;

import org.apache.hcatalog.messaging.AddPartitionMessage;
import org.apache.hcatalog.messaging.CreateDatabaseMessage;
import org.apache.hcatalog.messaging.CreateTableMessage;
import org.apache.hcatalog.messaging.DropDatabaseMessage;
import org.apache.hcatalog.messaging.DropPartitionMessage;
import org.apache.hcatalog.messaging.DropTableMessage;
import org.apache.hcatalog.messaging.MessageDeserializer;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * MessageDeserializer implementation, for deserializing from JSON strings.
 * @deprecated Use/modify {@link org.apache.hive.hcatalog.messaging.json.JSONMessageDeserializer} instead
 */
public class JSONMessageDeserializer extends MessageDeserializer {

  static ObjectMapper mapper = new ObjectMapper(); // Thread-safe.

  static {
    mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
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
  public DropPartitionMessage getDropPartitionMessage(String messageBody) {
    try {
      return mapper.readValue(messageBody, JSONDropPartitionMessage.class);
    }
    catch (Exception exception) {
      throw new IllegalArgumentException("Could not construct DropPartitionMessage.", exception);
    }
  }
}
