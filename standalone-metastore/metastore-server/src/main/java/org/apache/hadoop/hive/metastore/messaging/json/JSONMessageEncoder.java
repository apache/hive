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

package org.apache.hadoop.hive.metastore.messaging.json;

import org.apache.hadoop.hive.metastore.messaging.MessageDeserializer;
import org.apache.hadoop.hive.metastore.messaging.MessageEncoder;
import org.apache.hadoop.hive.metastore.messaging.MessageSerializer;
import org.apache.hadoop.hive.metastore.messaging.json.gzip.GzipJSONMessageEncoder;

/**
 * The JSON implementation of the MessageFactory. Constructs JSON implementations of each
 * message-type.
 */
public class JSONMessageEncoder implements MessageEncoder {
  public static final String FORMAT = "json-0.2";

  private static MessageDeserializer deserializer = new JSONMessageDeserializer();
  private static MessageSerializer serializer = new MessageSerializer() {
  };

  private static volatile MessageEncoder instance;

  public static MessageEncoder getInstance() {
    if (instance == null) {
      synchronized (GzipJSONMessageEncoder.class) {
        if (instance == null) {
          instance = new JSONMessageEncoder();
        }
      }
    }
    return instance;
  }

  @Override
  public MessageDeserializer getDeserializer() {
    return deserializer;
  }

  @Override
  public MessageSerializer getSerializer() {
    return serializer;
  }

  /**
   * This is a format that's shipped, for any changes make sure that backward compatibiltiy
   * with existing messages in this format are taken care of.
   *
   */
  @Override
  public String getMessageFormat() {
    return FORMAT;
  }
}
