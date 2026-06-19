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

package org.apache.hadoop.hive.metastore.messaging.json.gzip;

import org.apache.hadoop.hive.metastore.messaging.MessageDeserializer;
import org.apache.hadoop.hive.metastore.messaging.MessageEncoder;
import org.apache.hadoop.hive.metastore.messaging.MessageFactory;
import org.apache.hadoop.hive.metastore.messaging.MessageSerializer;

/**
 * This implementation gzips and then Base64 encodes the message before writing it out.
 * This MessageEncoder will break the backward compatibility for hive replication v1 which uses webhcat endpoints.
 */
public class GzipJSONMessageEncoder implements MessageEncoder {
  public static final String FORMAT = "gzip(json-2.0)";

  static {
    MessageFactory.register(FORMAT, GzipJSONMessageEncoder.class);
  }

  private static DeSerializer deSerializer = new DeSerializer();
  private static Serializer serializer = new Serializer();

  private static volatile MessageEncoder instance;

  public static MessageEncoder getInstance() {
    if (instance == null) {
      synchronized (GzipJSONMessageEncoder.class) {
        if (instance == null) {
          instance = new GzipJSONMessageEncoder();
        }
      }
    }
    return instance;
  }

  @Override
  public MessageDeserializer getDeserializer() {
    return deSerializer;
  }

  @Override
  public MessageSerializer getSerializer() {
    return serializer;
  }

  @Override
  public String getMessageFormat() {
    return FORMAT;
  }
}
