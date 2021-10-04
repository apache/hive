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

import org.apache.hadoop.hive.metastore.messaging.EventMessage;
import org.apache.hadoop.hive.metastore.messaging.MessageSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.zip.GZIPOutputStream;

class Serializer implements MessageSerializer {
  private static final Logger LOG = LoggerFactory.getLogger(Serializer.class.getName());

  @Override
  public String serialize(EventMessage message) {
    return serialize(MessageSerializer.super.serialize(message));
  }

  @Override
  public String serialize(String msg) {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      GZIPOutputStream gout = new GZIPOutputStream(baos);
      gout.write(msg.getBytes(StandardCharsets.UTF_8));
      gout.close();
      byte[] compressed = baos.toByteArray();
      return new String(Base64.getEncoder().encode(compressed), StandardCharsets.UTF_8);
    } catch (IOException e) {
      LOG.error("could not use gzip output stream", e);
      LOG.debug("message " + msg);
      throw new RuntimeException("could not use the gzip output Stream", e);
    }
  }
}
